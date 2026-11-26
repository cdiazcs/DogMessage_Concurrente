"""Motor de consenso Raft sobre DM2 binario."""
from __future__ import annotations

import json
import random
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import dm2_codec as dm2
from chat_feed import diagnostic_bytes
from storage import NodeStorage, PersistentState, StorageError

Endpoint = Tuple[str, int]
FAULT_FILE = "fault.json"
CLIENT_WAIT = 1.5  # Menor que el presupuesto de lectura de 5 s del cliente.


class RaftEngine:
    """Implementa la interfaz RpcHandler de node.py (request_vote, append_entries, client_command)."""

    def __init__(self, node_id: str, peers: Dict[str, Endpoint], data_dir: Path, client: Any) -> None:
        self.node_id = node_id
        self.peers = dict(peers)  # {peer_id: (host, port)}
        self.client = client  # Debe ofrecer call_rpc(endpoint, expected_peer, sender_id, payload).
        self.data_dir = Path(data_dir)
        self.storage = NodeStorage(self.data_dir, node_id)

        self.lock = threading.RLock()
        self.storage.acquire()
        try:
            persisted = self.storage.load()
        except BaseException:
            self.storage.close()
            raise
        self.current_term: int = persisted.current_term
        self.voted_for: Optional[str] = persisted.voted_for
        self.log: List[dm2.LogEntry] = persisted.log

        # Estado volátil Raft
        self.state: str = "FOLLOWER"  # FOLLOWER | CANDIDATE | LEADER
        self.leader_id: Optional[str] = None
        self.commit_index: int = persisted.commit_index  # Fase I: se recupera de disco, no len(log).
        self.last_applied: int = 0

        # Estado volátil del Líder
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        self._in_flight: Dict[str, threading.Thread] = {}
        self._votes: set[str] = set()
        self._election_started_at = 0.0

        # Idempotencia al aplicar: (clientId, clientRequestId) -> primer índice aplicado
        self.applied_requests: Dict[Tuple[str, bytes], int] = {}
        self._commit_cond = threading.Condition(self.lock)

        # Simulación de fallos (Integrante 2): se controla con <data-dir>/fault.json
        self.isolated: bool = False
        self.artificial_delay_ms: int = 0
        self._fault_content: Optional[bytes] = None

        # Métricas
        self._metrics: Dict[str, Any] = {
            "algorithm": "RAFT",
            "elections_started": 0,
            "elections_won": 0,
            "rpc_sent": 0,
            "rpc_received": 0,
            "commands_committed": 0,
            "last_election_ms": 0.0,
        }

        self._running = False
        self._stopped = False
        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._random_timeout()
        self._threads: List[threading.Thread] = []
        self._apply_committed_entries(live=False)

    @property
    def majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def _random_timeout(self) -> float:
        return random.uniform(0.35, 0.70)

    def _persist(self) -> None:
        try:
            self.storage.save(PersistentState(self.current_term, self.voted_for, self.commit_index, self.log))
        except OSError as error:
            self._fail_stop(str(error))
            raise

    def _fail_stop(self, reason: str) -> None:
        self._running = False
        self._stopped = True
        self._commit_cond.notify_all()
        print(f"[RAFT {self.node_id}] stopped: {reason}", file=sys.stderr, flush=True)

    def _last_log_index_and_term(self) -> Tuple[int, int]:
        if not self.log:
            return 0, 0
        return len(self.log), self.log[-1].term

    def _log_event(self, event: str) -> None:
        print(f"[RAFT {self.node_id}] role={self.state} term={self.current_term} leader={self.leader_id or '-'} "
              f"event={event} commitIndex={self.commit_index} lastApplied={self.last_applied} "
              f"lastLogIndex={len(self.log)}", flush=True)

    def start(self) -> None:
        with self.lock:
            if self._running or self._stopped:
                return
            self._running = True
            self._last_heartbeat = time.monotonic()
            self._election_timeout = self._random_timeout()
            self._log_event("startup")

            self._threads = [
                threading.Thread(target=self._election_loop, name=f"raft-elec-{self.node_id}", daemon=True),
                threading.Thread(target=self._heartbeat_loop, name=f"raft-hb-{self.node_id}", daemon=True),
            ]
            for thread in self._threads:
                thread.start()

    def stop(self, join_timeout: float = 0.0) -> None:
        with self.lock:
            self._running = False
            self._stopped = True
            self._commit_cond.notify_all()
            self.storage.close()
            threads = [*self._threads, *self._in_flight.values()]
        deadline = time.monotonic() + join_timeout
        for thread in threads:
            remaining = deadline - time.monotonic()
            if remaining > 0 and thread is not threading.current_thread():
                thread.join(remaining)

    def set_fault_state(self, isolated: Optional[bool] = None, delay_ms: Optional[int] = None) -> Dict[str, Any]:
        with self.lock:
            if self._stopped:
                return {"isolated": self.isolated, "delay_ms": self.artificial_delay_ms}
            if isolated is not None:
                changed = self.isolated != bool(isolated)
                self.isolated = bool(isolated)
                if not self.isolated:
                    self._last_heartbeat = time.monotonic()
                if changed:
                    self._log_event("isolated" if self.isolated else "healed")
            if delay_ms is not None:
                self.artificial_delay_ms = max(0, int(delay_ms))
            return {"isolated": self.isolated, "delay_ms": self.artificial_delay_ms}

    def _poll_fault_file(self) -> None:
        """Lee <data-dir>/fault.json fuera del lock; solo aplica cambios cuando el archivo cambia."""
        path = self.data_dir / FAULT_FILE
        try:
            content: Optional[bytes] = path.read_bytes()
        except FileNotFoundError:
            content = None
        except OSError:
            return
        # Se compara el contenido, no el mtime: en FAT/Windows dos escrituras seguidas comparten mtime.
        if content == self._fault_content:
            return
        config: Dict[str, Any] = {}
        if content is not None:
            try:
                config = json.loads(content.decode("utf-8"))
            except ValueError:
                return  # Escritura a medias: se reintenta en la próxima vuelta.
        self._fault_content = content
        self.set_fault_state(isolated=bool(config.get("isolated", False)), delay_ms=int(config.get("delay_ms", 0)))

    def leader(self) -> Optional[str]:
        with self.lock:
            return self.leader_id

    def chat_snapshot(self, after):
        with self.lock:
            return self.last_applied, tuple(
                (index + 1, self.log[index].command)
                for index in range(min(after, self.last_applied), self.last_applied)
                if self.log[index].kind == dm2.COMMAND
            )

    def committedEntries(self) -> List[dm2.LogEntry]:
        with self.lock:
            return list(self.log[: self.commit_index])

    def metrics(self) -> Dict[str, Any]:
        with self.lock:
            last_idx, last_term = self._last_log_index_and_term()
            return {
                **self._metrics,
                "nodeId": self.node_id,
                "state": self.state,
                "term": self.current_term,
                "leaderId": self.leader_id,
                "logLength": last_idx,
                "lastLogTerm": last_term,
                "commitIndex": self.commit_index,
                "isolated": self.isolated,
            }

    # --- Bucles de fondo ---

    def _election_loop(self) -> None:
        while True:
            time.sleep(0.03)
            with self.lock:
                if not self._running:
                    return
            self._poll_fault_file()
            with self.lock:
                if not self._running:
                    return
                if self.isolated or self.state == "LEADER":
                    continue
                elapsed = time.monotonic() - self._last_heartbeat
                if elapsed >= self._election_timeout:
                    try:
                        self._start_election()
                    except OSError:
                        return  # _persist ya detuvo el nodo.

    def _heartbeat_loop(self) -> None:
        while True:
            time.sleep(0.10)
            with self.lock:
                if not self._running:
                    return
                if self.isolated or self.state != "LEADER":
                    continue
            self._replicate_to_all_peers()

    def _start_election(self) -> None:
        if self.current_term == dm2.R64_MAX:
            self._fail_stop("término agotado")
            return
        self._election_started_at = time.perf_counter()
        self.state = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.node_id
        self._votes = {self.node_id}
        self.leader_id = None
        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._random_timeout()
        self._metrics["elections_started"] += 1
        self._persist()
        self._log_event("election")

        if len(self._votes) >= self.majority:
            self._become_leader()
            return

        last_idx, last_term = self._last_log_index_and_term()
        request = dm2.RequestVote(self.current_term, last_idx, last_term)
        for peer_id in self.peers:
            self._send(peer_id, request, "CANDIDATE", 0.30)

    def _send(self, peer_id: str, request, role: str, timeout: float) -> None:
        # La reserva cubre elecciones y replicación, incluso entre cambios de término.
        if peer_id in self._in_flight:
            return

        def exchange() -> None:
            try:
                with self.lock:
                    if not self._is_current(request.term, role):
                        return
                    self._metrics["rpc_sent"] += 1
                reply = self._call(peer_id, self.peers[peer_id], request, timeout)
                with self.lock:
                    if self._stopped or self.isolated:
                        return
                    try:
                        self._accept_response(peer_id, request, role, reply)
                    except OSError:
                        # _persist ya detuvo el motor y registró el motivo.
                        return
            finally:
                with self.lock:
                    del self._in_flight[peer_id]

        thread = threading.Thread(target=exchange, name=f"raft-rpc-{self.node_id}-{peer_id}", daemon=True)
        self._in_flight[peer_id] = thread
        try:
            thread.start()
        except RuntimeError:
            del self._in_flight[peer_id]
            raise

    def _is_current(self, term: int, role: str) -> bool:
        return not self._stopped and not self.isolated and self.current_term == term and self.state == role

    def _accept_response(self, peer_id: str, request, role: str, reply) -> None:
        if reply is None or reply.term < request.term:
            return
        self._metrics["rpc_received"] += 1
        if reply.term > self.current_term:
            self._step_down(reply.term)
            return
        if not self._is_current(request.term, role):
            return
        if isinstance(reply, dm2.VoteResponse):
            if reply.vote_granted:
                self._votes.add(peer_id)
                if len(self._votes) >= self.majority:
                    self._become_leader()
        elif reply.success:
            matched = max(self.match_index[peer_id], reply.match_index)
            self.match_index[peer_id] = matched
            self.next_index[peer_id] = matched + 1
            self._advance_commit_index()
        else:
            self.next_index[peer_id] = max(self.match_index[peer_id] + 1, request.prev_log_index)

    def _call(self, peer_id: str, addr: Endpoint, request, timeout: float):
        """RPC fuera del lock. Devuelve el payload validado o None si el intento falló."""
        try:
            client = self.client.with_timeout(timeout) if hasattr(self.client, "with_timeout") else self.client
            payload = client.call_rpc(addr, peer_id, self.node_id, request).payload
        except (OSError, ValueError) as error:
            if str(error).startswith("Respuesta descartada"):
                # Fallo de contrato (correlación, término, matchIndex): visible en los logs.
                print(f"[RAFT {self.node_id}] peer={peer_id} event=rpc-discarded reason={error}", flush=True)
            return None
        if isinstance(payload, dm2.ErrorMsg):
            print(f"[RAFT {self.node_id}] peer={peer_id} event=rpc-error code={payload.code}", flush=True)
            return None
        return payload

    def _become_leader(self) -> None:
        first_new = len(self.log) + 1
        self.log.append(dm2.LogEntry.no_op(self.current_term))
        self._persist()
        self.next_index = dict.fromkeys(self.peers, first_new)
        self.match_index = dict.fromkeys(self.peers, 0)
        self.state = "LEADER"
        self.leader_id = self.node_id
        self._metrics["elections_won"] += 1
        self._metrics["last_election_ms"] = round((time.perf_counter() - self._election_started_at) * 1000, 2)
        self._log_event("majority")
        self._advance_commit_index()
        self._commit_cond.notify_all()
        self._replicate_to_all_peers()

    def _step_down(self, new_term: int) -> None:
        higher_term = new_term > self.current_term
        if higher_term:
            self.current_term = new_term
            self.voted_for = None
            self._persist()
        self.state = "FOLLOWER"
        self.leader_id = None
        self._votes.clear()
        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._random_timeout()
        if higher_term:
            self._log_event("higher-term")
        self._commit_cond.notify_all()

    # --- Replicación AppendEntries ---

    def _replicate_to_all_peers(self) -> None:
        with self.lock:
            if not self._is_current(self.current_term, "LEADER"):
                return
            for peer_id in self.peers:
                if peer_id not in self._in_flight:
                    self._send(peer_id, self._batch(self.next_index[peer_id]), "LEADER", 0.35)

    def _batch(self, next_idx: int) -> dm2.AppendEntries:
        """Lote acotado por 256 entradas y por el tamaño máximo de cuerpo DM2 (igual que Java)."""
        prev_log_index = min(max(next_idx, 1), len(self.log) + 1) - 1
        prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 else 0
        budget = dm2.MAX_BODY - dm2.HEADER_BYTES - len(self.node_id) - dm2.APPEND_FIXED_BYTES
        entries: List[dm2.LogEntry] = []
        for index in range(prev_log_index, min(len(self.log), prev_log_index + dm2.MAX_APPEND_ENTRIES)):
            entry = self.log[index]
            size = dm2.entry_size(entry)
            if size > budget:
                break
            entries.append(entry)
            budget -= size
        return dm2.AppendEntries(self.current_term, prev_log_index, prev_log_term, self.commit_index, tuple(entries))

    def _advance_commit_index(self) -> None:
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1].term != self.current_term:
                continue
            replicated_count = 1 + sum(1 for pid in self.peers if self.match_index.get(pid, 0) >= n)
            if replicated_count >= self.majority:
                self.commit_index = n
                self._persist()
                self._apply_committed_entries()
                self._commit_cond.notify_all()
                break

    def _apply_committed_entries(self, live: bool = True) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            effect = False
            if entry.kind == dm2.COMMAND:
                key = (entry.client_id, entry.client_request_id)
                if key not in self.applied_requests:
                    self.applied_requests[key] = self.last_applied
                    effect = True
                    self._metrics["commands_committed"] += 1
            if live:
                kind = "COMMAND" if entry.kind == dm2.COMMAND else "NO_OP"
                print(f"[RAFT {self.node_id}] entryIndex={self.last_applied} entryTerm={entry.term} kind={kind} "
                      f"commitIndex={self.commit_index} lastApplied={self.last_applied} effect={str(effect).lower()} "
                      f"event=applied", flush=True)

    # --- Manejadores RPC entrantes (invocados por node.Dispatcher) ---

    def _delay_before_rpc(self) -> None:
        with self.lock:
            delay = 0 if self._stopped or self.isolated else self.artificial_delay_ms
        if delay:
            time.sleep(delay / 1000.0)

    def _available(self) -> bool:
        if self._stopped:
            raise OSError("Raft detenido")
        if self.isolated:
            return False  # Partición simulada: no hay respuesta, igual que un enlace caído.
        self._metrics["rpc_received"] += 1
        return True

    def request_vote(self, header: dm2.Header, request: dm2.RequestVote) -> Optional[dm2.VoteResponse]:
        self._delay_before_rpc()
        with self.lock:
            if not self._available():
                return None
            candidate_id = header.sender_id

            if request.term > self.current_term:
                self._step_down(request.term)

            if request.term < self.current_term:
                return dm2.VoteResponse(self.current_term, False)

            my_last_idx, my_last_term = self._last_log_index_and_term()
            log_ok = (request.last_log_term > my_last_term) or (
                request.last_log_term == my_last_term and request.last_log_index >= my_last_idx
            )

            if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
                self.voted_for = candidate_id
                self._last_heartbeat = time.monotonic()
                self._persist()
                return dm2.VoteResponse(self.current_term, True)

            return dm2.VoteResponse(self.current_term, False)

    def append_entries(self, header: dm2.Header, request: dm2.AppendEntries) -> Optional[dm2.AppendResponse]:
        self._delay_before_rpc()
        with self.lock:
            if not self._available():
                return None
            leader_id = header.sender_id
            prev_log_index = request.prev_log_index

            if request.term < self.current_term or (
                request.term == self.current_term and self.state == "LEADER" and leader_id != self.node_id
            ):
                return dm2.AppendResponse(self.current_term, False, 0)

            if request.term > self.current_term or self.state != "FOLLOWER":
                self._step_down(request.term)

            if self.leader_id != leader_id:
                self.leader_id = leader_id
                self._log_event("append" if request.entries else "heartbeat")
            self._last_heartbeat = time.monotonic()

            # Verificar consistencia con prevLogIndex / prevLogTerm (también en heartbeats)
            if prev_log_index > len(self.log):
                return dm2.AppendResponse(self.current_term, False, 0)

            if prev_log_index > 0 and self.log[prev_log_index - 1].term != request.prev_log_term:
                return dm2.AppendResponse(self.current_term, False, 0)

            # Añadir nuevas entradas evitando duplicados; conservar el sufijo si no hay conflicto
            dirty = False
            for offset, entry in enumerate(request.entries):
                idx = prev_log_index + 1 + offset
                if idx <= len(self.log):
                    if self.log[idx - 1].term != entry.term:
                        if idx <= self.commit_index:
                            reason = "Conflicto con el prefijo comprometido"
                            self._fail_stop(reason)
                            raise OSError(reason)
                        self.log = self.log[: idx - 1]
                        self.log.append(entry)
                        dirty = True
                else:
                    self.log.append(entry)
                    dirty = True

            # Contrato DM2: matchIndex = prevLog.index + entryCount (no len(log)).
            match_index = request.last_index
            new_commit = max(self.commit_index, min(request.leader_commit, match_index))
            if new_commit != self.commit_index:
                self.commit_index = new_commit
                dirty = True

            if dirty:
                self._persist()
            self._apply_committed_entries()
            self._commit_cond.notify_all()

            return dm2.AppendResponse(self.current_term, True, match_index)

    # --- Comandos de cliente ---

    def _find_command(self, client_id: str, request_id: bytes) -> Optional[int]:
        for index, entry in enumerate(self.log, start=1):
            if entry.kind == dm2.COMMAND and entry.client_id == client_id and entry.client_request_id == request_id:
                return index
        return None

    def client_command(self, header: dm2.Header, request: dm2.ClientCommand) -> Optional[dm2.ClientResult]:
        client_id, request_id = header.sender_id, header.request_id
        self._delay_before_rpc()
        with self.lock:
            if not self._available():
                return None

            # Sin reenvío interno (igual que Java): el cliente sigue leaderHint.
            if self.state != "LEADER":
                return dm2.ClientResult(dm2.NOT_LEADER, 0, self.leader_id)

            # Idempotencia por (clientId, clientRequestId), pendiente o comprometido.
            index = self._find_command(client_id, request_id)
            if index is not None and self.log[index - 1].command != request.command:
                return dm2.ClientResult(dm2.REQUEST_ID_CONFLICT, 0)
            if index is None:
                self.log.append(dm2.LogEntry.of_command(self.current_term, client_id, request_id, request.command))
                self._persist()
                index = len(self.log)
                self._advance_commit_index()  # Clúster de un nodo: confirma de inmediato.
            term = self.current_term

            if index > self.commit_index:
                self._replicate_to_all_peers()

        deadline = time.monotonic() + CLIENT_WAIT
        with self.lock:
            while True:
                if self._stopped:
                    return None
                entry = self.log[index - 1] if index <= len(self.log) else None
                still_ours = entry is not None and (entry.client_id, entry.client_request_id, entry.command) == (
                    client_id, request_id, request.command
                )
                if still_ours and index <= self.commit_index:
                    return dm2.ClientResult(dm2.COMMITTED, index)
                if self.state != "LEADER" or self.current_term != term:
                    return dm2.ClientResult(dm2.NOT_LEADER, 0, self.leader_id)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return dm2.ClientResult(dm2.RETRY, 0)
                self._commit_cond.wait(timeout=remaining)


def inspect(data_dir: Path, node_id: str) -> int:
    """Imprime el estado persistido con el mismo formato que NodeMain --inspect de Java."""
    try:
        storage = NodeStorage(Path(data_dir), node_id)
        storage.acquire()
        try:
            state = storage.load()
        finally:
            storage.close()
    except (StorageError, OSError) as error:
        print(f"ERROR {error}")
        return 1
    applied: Dict[Tuple[str, bytes], int] = {}
    for index, entry in enumerate(state.log[: state.commit_index], start=1):
        if entry.kind == dm2.COMMAND:
            applied.setdefault((entry.client_id, entry.client_request_id), index)
    print(f"role=FOLLOWER term={state.current_term} leader=null commitIndex={state.commit_index} "
          f"lastApplied={state.commit_index} lastLogIndex={len(state.log)} "
          f"appliedCommands=[{', '.join(str(i) for i in applied.values())}]")
    for index, entry in enumerate(state.log, start=1):
        kind = "COMMAND" if entry.kind == dm2.COMMAND else "NO_OP"
        status = "COMMITTED" if index <= state.commit_index else "PENDING"
        client = entry.client_id if entry.kind == dm2.COMMAND else "null"
        request = str(uuid.UUID(bytes=entry.client_request_id)) if entry.kind == dm2.COMMAND else "null"
        command = diagnostic_bytes(entry.command) if entry.kind == dm2.COMMAND else "-"
        print(f"index={index} term={entry.term} type={kind} status={status} client={client} requestId={request} "
              f"command={command}")
    return 0
