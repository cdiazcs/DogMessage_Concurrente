import dogmessenger.chat.Feed
import java.io.DataOutputStream
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Socket
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

/** Motor persistente con replicación opcional para los bancos Raft. */
internal class PersistentRaftNode(
    private val id: String,
    members: Map<String, InetSocketAddress>,
    private val storage: RaftStorage,
    private val now: () -> Long = System::nanoTime,
    private val replication: Boolean = false,
    private val electionDelayMs: () -> Long = { ThreadLocalRandom.current().nextLong(500, 701) }
) : AutoCloseable {
    private enum class Role { FOLLOWER, CANDIDATE, LEADER }

    private val peers = members.toMap()
    private val majority = (peers.size + 1) / 2 + 1
    private val outbound = Executors.newFixedThreadPool(maxOf(1, peers.size))
    private val clock = Executors.newSingleThreadScheduledExecutor()
    private val watchdog = ScheduledThreadPoolExecutor(1).apply { removeOnCancelPolicy = true }
    private val inFlight = mutableMapOf<String, Socket>()
    private val votes = hashSetOf<String>()
    private var state = storage.snapshot()
    private var role = Role.FOLLOWER
    private var leader: String? = null
    private var stopped = false
    private var started = false
    private var electionDeadline = nextElectionDeadline()
    private var nextHeartbeat = 0L
    private var heartbeatBase: Pair<Long, Long> = 0L to 0L
    private data class Progress(var nextIndex: Long, var matchIndex: Long = 0)
    private val progress = mutableMapOf<String, Progress>()
    private val requests = mutableMapOf<Pair<String, UUID>, EntradaLog>()
    private var clientWaiters = 0
    @Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")
    private val monitor = this as java.lang.Object
    private var lastApplied = 0L
    private val appliedCommands = linkedMapOf<Pair<String, UUID>, EntradaLog>()

    init {
        require(raftIdPattern.matches(id) && id !in peers && peers.keys.all(raftIdPattern::matches))
        require(peers.values.toSet().size == peers.size && peers.values.all { !it.isUnresolved && it.port > 0 })
        rebuildRequests()
        applyCommit()
    }

    private fun rebuildRequests() {
        requests.clear()
        for (entry in state.log) {
            val client = entry.clientId ?: continue
            requests.putIfAbsent(client to checkNotNull(entry.requestId), entry)
        }
    }

    private fun applyCommit() {
        while (lastApplied < state.commitIndex) {
            val entry = state.log[lastApplied.toInt()]
            if (!entry.isNoOp) {
                val key = checkNotNull(entry.clientId) to checkNotNull(entry.requestId)
                appliedCommands.putIfAbsent(key, entry)
            }
            lastApplied = entry.index
        }
    }

    private fun nextElectionDeadline(): Long = now() + TimeUnit.MILLISECONDS.toNanos(electionDelayMs())
    private fun resetElection() { electionDeadline = nextElectionDeadline() }

    @Synchronized
    fun start() {
        check(!started && !stopped)
        started = true
        resetElection()
        event("startup")
        clock.scheduleWithFixedDelay(::tick, 0, 20, TimeUnit.MILLISECONDS)
    }

    @Synchronized
    fun tick() {
        if (stopped) return
        try {
            val instant = now()
            if (role == Role.LEADER) {
                if (instant - nextHeartbeat >= 0) sendHeartbeats(instant)
            } else if (instant - electionDeadline >= 0) {
                beginElection()
            }
        } catch (_: IOException) {
            // persist ya cerró la actividad electoral y registró el error.
        }
    }

    private fun beginElection() {
        if (state.currentTerm == Long.MAX_VALUE) {
            failStop("término agotado")
            return
        }
        persist(state.copy(currentTerm = state.currentTerm + 1, votedFor = id))
        role = Role.CANDIDATE
        leader = null
        votes.clear()
        votes.add(id)
        resetElection()
        event("election")
        if (votes.size >= majority) {
            becomeLeader()
        } else {
            val tail = state.log.lastOrNull()
            val vote = Dm2Protocol.Vote(state.currentTerm, tail?.index ?: 0, tail?.term ?: 0)
            peers.keys.forEach { submit(it, vote, vote.term, Role.CANDIDATE) }
        }
    }

    private fun becomeLeader() {
        val tail = state.log.lastOrNull()
        val index = tail?.index ?: 0L
        heartbeatBase = index to (tail?.term ?: 0L)
        // El modo electoral conserva su NO_OP local sin replicarlo.
        persist(state.copy(log = state.log + EntradaLog(index + 1, state.currentTerm)))
        progress.clear()
        if (replication) peers.keys.forEach { progress[it] = Progress(state.log.size + 1L) }
        role = Role.LEADER
        leader = id
        event("majority")
        if (replication) advanceLeaderCommit()
        sendHeartbeats(now())
    }

    private fun sendHeartbeats(instant: Long) {
        nextHeartbeat = instant + TimeUnit.MILLISECONDS.toNanos(125)
        if (replication) {
            peers.keys.forEach(::replicate)
        } else {
            val (index, term) = heartbeatBase
            val append = Dm2Protocol.Append(state.currentTerm, index, term, state.commitIndex)
            peers.keys.forEach { submit(it, append, append.term, Role.LEADER) }
        }
    }

    private fun replicate(peer: String) {
        if (peer in inFlight) return
        val next = progress.getValue(peer).nextIndex
        val previous = next - 1
        val previousTerm = if (previous == 0L) 0L else state.log[previous.toInt() - 1].term
        val entries = ArrayList<EntradaLog>()
        var budget = Dm2Protocol.appendBudget(id)
        val start = previous.toInt()
        for (i in start until minOf(state.log.size.toLong(), previous + Dm2Protocol.MAX_APPEND_ENTRIES).toInt()) {
            val entry = state.log[i]
            if (entry.dm2Size > budget) break
            entries.add(entry)
            budget -= entry.dm2Size
        }
        val request = Dm2Protocol.Append(state.currentTerm, previous, previousTerm, state.commitIndex, entries)
        submit(peer, request, request.term, Role.LEADER)
    }

    private fun replicationResult(peer: String, reply: Dm2Protocol.Reply) {
        val follower = progress.getValue(peer)
        if (reply.success) {
            follower.matchIndex = maxOf(follower.matchIndex, reply.matchIndex)
            follower.nextIndex = follower.matchIndex + 1
            advanceLeaderCommit()
            if (follower.nextIndex <= state.log.size) replicate(peer)
        } else {
            val next = maxOf(follower.matchIndex + 1, follower.nextIndex - 1)
            if (next < follower.nextIndex) {
                follower.nextIndex = next
                replicate(peer)
            }
        }
    }

    private fun advanceLeaderCommit() {
        val matched = (progress.values.map { it.matchIndex } + state.log.size.toLong()).sortedDescending()
        val quorumIndex = matched[majority - 1]
        if (quorumIndex > state.commitIndex && state.log[quorumIndex.toInt() - 1].term == state.currentTerm) {
            persist(state.copy(commitIndex = quorumIndex))
            applyCommit()
            event("commit")
            monitor.notifyAll()
        }
    }

    private fun submit(peer: String, request: Dm2Protocol.Request, term: Long, expectedRole: Role) {
        if (peer in inFlight) return
        val socket = Socket()
        inFlight[peer] = socket
        outbound.execute {
            var reply: Dm2Protocol.Reply? = null
            try {
                socket.use {
                    if (!synchronized(this) { isCurrent(term, expectedRole) }) return@execute
                    val requestId = Dm2Protocol.generarRequestId()
                    val deadline = synchronized(this) {
                        if (stopped) return@execute
                        watchdog.schedule({ runCatching { socket.close() } }, 900, TimeUnit.MILLISECONDS)
                    }
                    try {
                        socket.connect(peers.getValue(peer), 250)
                        Dm2Protocol.writeRequest(DataOutputStream(socket.getOutputStream()), id, requestId, request)
                        reply = Dm2Protocol.readReply(Dm2Protocol.readFrame(socket, 300), peer, requestId, request)
                    } finally {
                        deadline.cancel(false)
                    }
                }
            } catch (_: IOException) {
                // Un fallo de transporte conserva el plazo electoral vigente.
            } catch (_: IllegalArgumentException) {
                // Una respuesta inválida nunca modifica el término ni el voto.
            } finally {
                synchronized(this) {
                    inFlight.remove(peer)
                    try {
                        val received = reply
                        if (!stopped && received != null) {
                            if (received.term > state.currentTerm) {
                                observeHigherTerm(received.term)
                            } else if (isCurrent(term, Role.CANDIDATE) && expectedRole == Role.CANDIDATE &&
                                received.success && votes.add(peer) && votes.size >= majority) {
                                becomeLeader()
                            } else if (replication && request is Dm2Protocol.Append && isCurrent(term, Role.LEADER)) {
                                replicationResult(peer, received)
                            }
                        }
                    } catch (_: IOException) {
                        // El fallo de disco ya detuvo el motor.
                    }
                }
            }
        }
    }

    private fun isCurrent(term: Long, expectedRole: Role) =
        !stopped && state.currentTerm == term && role == expectedRole

    private fun observeHigherTerm(term: Long) {
        persist(state.copy(currentTerm = term, votedFor = null))
        if (role == Role.LEADER) resetElection()
        role = Role.FOLLOWER
        leader = null
        votes.clear()
        event("higher-term")
        monitor.notifyAll()
    }

    data class VoteResult(val term: Long, val granted: Boolean)

    @Synchronized
    fun requestVote(candidate: String, term: Long, index: Long, logTerm: Long): VoteResult {
        ensureAvailable()
        require(candidate in peers && Dm2Protocol.validPosition(term, index, logTerm))
        if (term > state.currentTerm) observeHigherTerm(term)
        val local = state.log.lastOrNull()
        val lastTerm = local?.term ?: 0L
        val upToDate = logTerm > lastTerm || (logTerm == lastTerm && index >= (local?.index ?: 0L))
        val granted = term == state.currentTerm && upToDate &&
            (state.votedFor == null || state.votedFor == candidate)
        if (granted) {
            if (state.votedFor != candidate) persist(state.copy(votedFor = candidate))
            resetElection()
        }
        return VoteResult(state.currentTerm, granted)
    }

    @Synchronized
    fun appendEntries(sender: String, append: Dm2Protocol.Append): Dm2Protocol.Reply {
        ensureAvailable()
        require(sender in peers && Dm2Protocol.validPosition(append.term, append.index, append.logTerm) &&
            append.commit >= 0)
        if (append.term > state.currentTerm) observeHigherTerm(append.term)
        if (append.term < state.currentTerm || role == Role.LEADER) return Dm2Protocol.Reply(state.currentTerm, false)

        val changed = role != Role.FOLLOWER || leader != sender
        role = Role.FOLLOWER
        leader = sender
        votes.clear()
        resetElection()
        if (changed) event("heartbeat")
        val matches = append.index == 0L ||
            (append.index <= state.log.size && state.log[append.index.toInt() - 1].term == append.logTerm)
        if (!matches || (!replication && append.entries.isNotEmpty())) {
            return Dm2Protocol.Reply(state.currentTerm, false)
        }

        var firstReplacement = -1
        for ((offset, entry) in append.entries.withIndex()) {
            val existing = state.log.getOrNull(entry.index.toInt() - 1)
            if (existing == null || existing.term != entry.term) {
                if (entry.index <= state.commitIndex) inconsistent("conflicto con el prefijo comprometido")
                if (firstReplacement == -1) firstReplacement = offset
            } else if (!existing.sameEntry(entry)) {
                inconsistent("contenido distinto en el mismo índice y término")
            }
        }
        val merged = if (firstReplacement == -1) state.log
            else state.log.take(append.index.toInt() + firstReplacement) + append.entries.drop(firstReplacement)

        val commit = maxOf(state.commitIndex, minOf(append.commit, append.lastIndex))
        if (merged !== state.log || commit != state.commitIndex) {
            val changedLog = merged !== state.log
            persist(state.copy(log = merged, commitIndex = commit))
            if (changedLog) rebuildRequests()
            applyCommit()
            event("replicated")
            monitor.notifyAll()
        }
        return Dm2Protocol.Reply(state.currentTerm, true, append.lastIndex)
    }

    private fun inconsistent(reason: String): Nothing {
        failStop(reason)
        throw IOException(reason)
    }

    private data class ClientResult(val status: Int, val index: Long = 0, val hint: String? = null)

    @Synchronized
    private fun clientCommand(client: String, token: UUID, bytes: ByteArray): ClientResult {
        ensureAvailable()
        if (!replication) return ClientResult(2)
        if (role != Role.LEADER) return ClientResult(1, hint = leader)
        val known = requests[client to token]
        if (known != null && !known.hasCommand(bytes)) return ClientResult(3)
        if (known != null && known.index <= state.commitIndex) return ClientResult(0, known.index)
        if (clientWaiters >= 8) return ClientResult(2)

        val entry = known ?: EntradaLog(state.log.size + 1L, state.currentTerm, client, token, bytes).also {
            persist(state.copy(log = state.log + it))
            requests[client to token] = it
            advanceLeaderCommit()
        }
        peers.keys.forEach(::replicate)
        val term = state.currentTerm
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(750)
        clientWaiters++
        try {
            while (true) {
                ensureAvailable()
                val current = requests[client to token]
                if (current != null && current.hasCommand(bytes) && current.index <= state.commitIndex) {
                    return ClientResult(0, current.index)
                }
                if (!isCurrent(term, Role.LEADER)) return ClientResult(2)
                val remaining = deadline - System.nanoTime()
                if (remaining <= 0) return ClientResult(2)
                TimeUnit.NANOSECONDS.timedWait(monitor, remaining)
            }
        } catch (error: InterruptedException) {
            Thread.currentThread().interrupt()
            throw IOException("Espera de cliente interrumpida", error)
        } finally {
            clientWaiters--
        }
    }

    private fun ensureAvailable() {
        if (stopped) throw IOException("Raft detenido")
    }

    private fun persist(next: RaftState) {
        try {
            storage.save(next)
            state = next
        } catch (error: IOException) {
            failStop("fallo de persistencia: ${error.message}")
            throw error
        }
    }

    private fun failStop(reason: String) {
        System.err.println("[RAFT $id] detenido por $reason")
        stop()
    }

    fun handleDm2(body: ByteArray, output: DataOutputStream) {
        val header = Dm2Protocol.leerHeader(body)
        if (header.type !in 1..0x7f) return
        fun error(code: Int) {
            Dm2Protocol.writeFrame(output, id, header.requestId, Dm2Protocol.TYPE_ERROR, header.algorithm) {
                it.writeByte(code)
            }
        }
        if (header.algorithm != Dm2Protocol.ALGORITHM_RAFT) return error(1)
        if (header.type !in 1..3) return error(2)
        val request = try {
            Dm2Protocol.readRequest(body, header)
        } catch (_: IOException) {
            return error(3)
        } catch (_: IllegalArgumentException) {
            return error(3)
        }
        if (request !is Dm2Protocol.ClientCommand && header.senderId !in peers) return error(4)
        Dm2Protocol.writeFrame(output, id, header.requestId, header.type or 0x80) { response ->
            when (request) {
                is Dm2Protocol.Vote -> {
                    val vote = requestVote(header.senderId, request.term, request.index, request.logTerm)
                    response.writeLong(vote.term)
                    response.writeBoolean(vote.granted)
                }
                is Dm2Protocol.Append -> {
                    val reply = appendEntries(header.senderId, request)
                    response.writeLong(reply.term)
                    response.writeBoolean(reply.success)
                    response.writeLong(reply.matchIndex)
                }
                is Dm2Protocol.ClientCommand -> {
                    val result = clientCommand(header.senderId, Dm2Protocol.requestUuid(header.requestId), request.bytes)
                    response.writeByte(result.status)
                    response.writeLong(result.index)
                    val hint = result.hint.orEmpty()
                    response.writeByte(hint.length)
                    response.write(hint.toByteArray(Charsets.US_ASCII))
                }
            }
        }
    }

    private fun event(reason: String) {
        println("[RAFT $id] role=$role term=${state.currentTerm} leader=${leader ?: "-"} " +
            "event=$reason commitIndex=${state.commitIndex}")
    }

    @Synchronized
    fun chatSnapshot(after: Long): Feed.Snapshot {
        val entries = ArrayList<Feed.Entry>()
        for (i in minOf(after, lastApplied).toInt() until lastApplied.toInt()) {
            val entry = state.log[i]
            if (!entry.isNoOp) entries.add(Feed.Entry(entry.index, entry.command))
        }
        return Feed.Snapshot(lastApplied, entries)
    }

    @Synchronized
    fun inspect(): String = buildString {
        appendLine("role=$role term=${state.currentTerm} votedFor=${state.votedFor} leader=$leader " +
            "commitIndex=${state.commitIndex} lastApplied=$lastApplied lastLogIndex=${state.log.size} " +
            "appliedCommands=${appliedCommands.values.map { it.index }}")
        for (entry in state.log) {
            val type = if (entry.isNoOp) "NO_OP" else "COMMAND"
            val status = if (entry.index <= state.commitIndex) "COMMITTED" else "PENDING"
            val command = if (entry.isNoOp) "-" else Feed.diagnosticBytes(entry.command)
            appendLine("index=${entry.index} term=${entry.term} type=$type status=$status " +
                "client=${entry.clientId} requestId=${entry.requestId} command=$command")
        }
    }.trimEnd()

    @Synchronized
    private fun stop() {
        if (stopped) return
        stopped = true
        monitor.notifyAll()
        clock.shutdownNow()
        watchdog.shutdownNow()
        inFlight.values.forEach { runCatching { it.close() } }
        outbound.shutdownNow()
    }

    override fun close() {
        stop()
        try {
            clock.awaitTermination(1, TimeUnit.SECONDS)
            outbound.awaitTermination(1, TimeUnit.SECONDS)
            watchdog.awaitTermination(1, TimeUnit.SECONDS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }
}
