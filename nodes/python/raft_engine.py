"""Motor de Consenso Raft interoperable para Dog Messenger."""
from __future__ import annotations

import random
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from protocol_dm2 import build_dm2
from storage import NodeStorage


class RaftEngine:
    def __init__(
        self,
        node_id: str,
        peers: Dict[str, Tuple[str, int]],
        rpc_sender: Callable[[str, Tuple[str, int], str, float], Optional[Dict[str, Any]]],
        data_dir: str = "data",
    ) -> None:
        self.node_id = node_id
        self.peers = peers  # {peer_id: (host, port)}
        self.rpc_sender = rpc_sender
        self.storage = NodeStorage(node_id, data_dir=data_dir)

        self.lock = threading.RLock()
        persisted = self.storage.load()
        self.current_term: int = persisted["currentTerm"]
        self.voted_for: Optional[str] = persisted["votedFor"]
        self.log: List[Dict[str, Any]] = persisted["log"]

        # Estado volátil Raft
        self.state: str = "FOLLOWER"  # FOLLOWER | CANDIDATE | LEADER
        self.leader_id: Optional[str] = None
        self.commit_index: int = len(self.log)  # Al recuperar, las entradas persistidas están listas para sincronizar
        self.last_applied: int = len(self.log)

        # Estado volátil del Líder
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Idempotencia y espera de clientes
        self.applied_requests: Dict[str, Dict[str, Any]] = {
            entry["requestId"]: entry for entry in self.log if "requestId" in entry
        }
        self._commit_cond = threading.Condition(self.lock)

        # Simulación de fallos (Integrante 2)
        self.isolated: bool = False
        self.artificial_delay_ms: int = 0

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
        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._random_timeout()
        self._threads: List[threading.Thread] = []

    def _random_timeout(self) -> float:
        return random.uniform(0.35, 0.70)

    def _persist(self) -> None:
        self.storage.save(self.current_term, self.voted_for, self.log)

    def _last_log_index_and_term(self) -> Tuple[int, int]:
        if not self.log:
            return 0, 0
        last = self.log[-1]
        return int(last["index"]), int(last["term"])

    def start(self) -> None:
        with self.lock:
            if self._running:
                return
            self._running = True
            self._last_heartbeat = time.monotonic()
            self._election_timeout = self._random_timeout()

        t_elec = threading.Thread(target=self._election_loop, name=f"raft-elec-{self.node_id}", daemon=True)
        t_hb = threading.Thread(target=self._heartbeat_loop, name=f"raft-hb-{self.node_id}", daemon=True)
        self._threads = [t_elec, t_hb]
        t_elec.start()
        t_hb.start()

    def stop(self) -> None:
        with self.lock:
            self._running = False
            self._commit_cond.notify_all()

    def set_fault_state(self, isolated: Optional[bool] = None, delay_ms: Optional[int] = None) -> Dict[str, Any]:
        with self.lock:
            if isolated is not None:
                self.isolated = bool(isolated)
                if not self.isolated:
                    self._last_heartbeat = time.monotonic()
            if delay_ms is not None:
                self.artificial_delay_ms = max(0, int(delay_ms))
            return {"isolated": self.isolated, "delay_ms": self.artificial_delay_ms}

    def leader(self) -> Optional[str]:
        with self.lock:
            return self.leader_id

    def committedEntries(self) -> List[Dict[str, Any]]:
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
                if self.isolated or self.state == "LEADER":
                    continue
                elapsed = time.monotonic() - self._last_heartbeat
                if elapsed >= self._election_timeout:
                    self._start_election()

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
        t0 = time.perf_counter()
        self.state = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._random_timeout()
        self._metrics["elections_started"] += 1
        self._persist()

        term_at_start = self.current_term
        last_idx, last_term = self._last_log_index_and_term()
        peers_snapshot = dict(self.peers)

        votes_granted = 1
        total_nodes = len(peers_snapshot) + 1
        majority = (total_nodes // 2) + 1

        if votes_granted >= majority:
            self._become_leader(t0)
            return

        def ask_vote(peer_id: str, addr: Tuple[str, int]) -> None:
            nonlocal votes_granted
            with self.lock:
                if self.isolated or self.state != "CANDIDATE" or self.current_term != term_at_start:
                    return
                self._metrics["rpc_sent"] += 1

            msg = build_dm2(
                "RAFT_REQUEST_VOTE",
                self.node_id,
                {
                    "term": term_at_start,
                    "candidateId": self.node_id,
                    "lastLogIndex": last_idx,
                    "lastLogTerm": last_term,
                },
            )
            resp = self.rpc_sender(peer_id, addr, msg, 0.30)
            if not resp:
                return

            payload = resp.get("payload", {})
            with self.lock:
                self._metrics["rpc_received"] += 1
                resp_term = int(payload.get("term", 0))
                if resp_term > self.current_term:
                    self._step_down(resp_term)
                    return
                if (
                    self.state == "CANDIDATE"
                    and self.current_term == term_at_start
                    and payload.get("voteGranted") is True
                ):
                    votes_granted += 1
                    if votes_granted >= majority:
                        self._become_leader(t0)

        for pid, paddr in peers_snapshot.items():
            threading.Thread(target=ask_vote, args=(pid, paddr), daemon=True).start()

    def _become_leader(self, election_t0: float) -> None:
        if self.state == "LEADER":
            return
        self.state = "LEADER"
        self.leader_id = self.node_id
        self._metrics["elections_won"] += 1
        self._metrics["last_election_ms"] = round((time.perf_counter() - election_t0) * 1000, 2)
        next_idx = len(self.log) + 1
        for pid in self.peers:
            self.next_index[pid] = next_idx
            self.match_index[pid] = 0
        threading.Thread(target=self._replicate_to_all_peers, daemon=True).start()

    def _step_down(self, new_term: int) -> None:
        self.current_term = new_term
        self.state = "FOLLOWER"
        self.voted_for = None
        self._last_heartbeat = time.monotonic()
        self._persist()

    # --- Replicación AppendEntries ---

    def _replicate_to_all_peers(self) -> None:
        with self.lock:
            if self.isolated or self.state != "LEADER":
                return
            peers_snapshot = dict(self.peers)

        for pid, addr in peers_snapshot.items():
            threading.Thread(target=self._send_append_entries, args=(pid, addr), daemon=True).start()

    def _send_append_entries(self, peer_id: str, addr: Tuple[str, int]) -> None:
        with self.lock:
            if self.isolated or self.state != "LEADER":
                return
            term = self.current_term
            next_idx = self.next_index.get(peer_id, len(self.log) + 1)
            prev_log_index = next_idx - 1
            prev_log_term = self.log[prev_log_index - 1]["term"] if prev_log_index > 0 and prev_log_index <= len(self.log) else 0
            entries = list(self.log[prev_log_index:])
            leader_commit = self.commit_index
            self._metrics["rpc_sent"] += 1

        msg = build_dm2(
            "RAFT_APPEND_ENTRIES",
            self.node_id,
            {
                "term": term,
                "leaderId": self.node_id,
                "prevLogIndex": prev_log_index,
                "prevLogTerm": prev_log_term,
                "entries": entries,
                "leaderCommit": leader_commit,
            },
        )
        resp = self.rpc_sender(peer_id, addr, msg, 0.35)
        if not resp:
            return

        payload = resp.get("payload", {})
        with self.lock:
            self._metrics["rpc_received"] += 1
            resp_term = int(payload.get("term", 0))
            if resp_term > self.current_term:
                self._step_down(resp_term)
                return
            if self.state != "LEADER" or self.current_term != term:
                return

            if payload.get("success") is True:
                match_idx = int(payload.get("matchIndex", prev_log_index + len(entries)))
                self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), match_idx)
                self.next_index[peer_id] = self.match_index[peer_id] + 1
                self._advance_commit_index()
            else:
                # Retroceder nextIndex para reparar logs divergentes tras partición o caída
                hint_index = payload.get("conflictIndex")
                if isinstance(hint_index, int) and hint_index >= 1:
                    self.next_index[peer_id] = min(next_idx - 1, hint_index) if next_idx > 1 else 1
                else:
                    self.next_index[peer_id] = max(1, next_idx - 1)

    def _advance_commit_index(self) -> None:
        total_nodes = len(self.peers) + 1
        majority = (total_nodes // 2) + 1
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1]["term"] != self.current_term:
                continue
            replicated_count = 1 + sum(1 for pid in self.peers if self.match_index.get(pid, 0) >= n)
            if replicated_count >= majority:
                self.commit_index = n
                self._apply_committed_entries()
                self._commit_cond.notify_all()
                break

    def _apply_committed_entries(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            req_id = entry.get("requestId")
            if req_id:
                self.applied_requests[req_id] = entry
            self._metrics["commands_committed"] += 1

    # --- Manejadores RPC entrantes ---

    def handle_request_vote(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self.lock:
            if self.isolated:
                return {"term": self.current_term, "voteGranted": False, "reason": "ISOLATED"}
            if self.artificial_delay_ms > 0:
                time.sleep(self.artificial_delay_ms / 1000.0)

            self._metrics["rpc_received"] += 1
            term = int(payload.get("term", 0))
            candidate_id = str(payload.get("candidateId", ""))
            last_log_index = int(payload.get("lastLogIndex", 0))
            last_log_term = int(payload.get("lastLogTerm", 0))

            if term > self.current_term:
                self._step_down(term)

            if term < self.current_term:
                return {"term": self.current_term, "voteGranted": False}

            my_last_idx, my_last_term = self._last_log_index_and_term()
            log_ok = (last_log_term > my_last_term) or (
                last_log_term == my_last_term and last_log_index >= my_last_idx
            )

            if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
                self.voted_for = candidate_id
                self._last_heartbeat = time.monotonic()
                self._persist()
                return {"term": self.current_term, "voteGranted": True}

            return {"term": self.current_term, "voteGranted": False}

    def handle_append_entries(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self.lock:
            if self.isolated:
                return {"term": self.current_term, "success": False, "reason": "ISOLATED"}
            if self.artificial_delay_ms > 0:
                time.sleep(self.artificial_delay_ms / 1000.0)

            self._metrics["rpc_received"] += 1
            term = int(payload.get("term", 0))
            leader_id = str(payload.get("leaderId", ""))
            prev_log_index = int(payload.get("prevLogIndex", 0))
            prev_log_term = int(payload.get("prevLogTerm", 0))
            entries = list(payload.get("entries", []))
            leader_commit = int(payload.get("leaderCommit", 0))

            if term < self.current_term:
                return {"term": self.current_term, "success": False, "matchIndex": len(self.log)}

            if term > self.current_term or self.state != "FOLLOWER":
                self._step_down(term)

            self.leader_id = leader_id
            self._last_heartbeat = time.monotonic()

            # Verificar consistencia con prevLogIndex / prevLogTerm
            if prev_log_index > len(self.log):
                return {
                    "term": self.current_term,
                    "success": False,
                    "conflictIndex": len(self.log) + 1,
                }

            if prev_log_index > 0:
                existing_term = self.log[prev_log_index - 1]["term"]
                if existing_term != prev_log_term:
                    # Truncar entradas conflictivas
                    self.log = self.log[: prev_log_index - 1]
                    self._persist()
                    return {
                        "term": self.current_term,
                        "success": False,
                        "conflictIndex": prev_log_index,
                    }

            # Añadir nuevas entradas evitando duplicados
            dirty = False
            for offset, entry in enumerate(entries):
                idx = prev_log_index + 1 + offset
                if idx <= len(self.log):
                    if self.log[idx - 1]["term"] != entry["term"]:
                        self.log = self.log[: idx - 1]
                        self.log.append(entry)
                        dirty = True
                else:
                    self.log.append(entry)
                    dirty = True

            if dirty:
                self._persist()

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log))
                self._apply_committed_entries()
                self._commit_cond.notify_all()

            return {
                "term": self.current_term,
                "success": True,
                "matchIndex": len(self.log),
            }

    # --- Propuesta de cliente ---

    def propose(self, command: str, request_id: str, timeout: float = 3.0) -> Dict[str, Any]:
        with self.lock:
            if self.isolated:
                return {"ok": False, "error": "NODE_ISOLATED", "leaderId": self.leader_id}

            # Idempotencia: si ya fue confirmado, devolver resultado inmediato
            if request_id in self.applied_requests:
                entry = self.applied_requests[request_id]
                return {
                    "ok": True,
                    "committed": True,
                    "duplicate": True,
                    "index": entry["index"],
                    "term": entry["term"],
                    "leaderId": self.leader_id,
                }

            # Si no somos líder, redirigir internamente al líder conocido o indicar redirección
            if self.state != "LEADER":
                target_leader = self.leader_id
                peer_addr = self.peers.get(target_leader) if target_leader else None
                if not peer_addr:
                    return {"ok": False, "error": "NO_LEADER", "leaderId": target_leader}

                msg = build_dm2(
                    "CLIENT_COMMAND",
                    self.node_id,
                    {"command": command, "requestId": request_id},
                )
                # Liberar lock durante reenvío al líder
                self.lock.release()
                try:
                    resp = self.rpc_sender(target_leader, peer_addr, msg, timeout)
                finally:
                    self.lock.acquire()

                if resp and "payload" in resp:
                    return resp["payload"]
                return {"ok": False, "error": "LEADER_UNREACHABLE", "leaderId": target_leader}

            # Somos líder: anexar al log y persistir antes de replicar
            new_index = len(self.log) + 1
            entry = {
                "index": new_index,
                "term": self.current_term,
                "requestId": request_id,
                "command": command,
            }
            self.log.append(entry)
            self._persist()

            # Si es un clúster de 1 nodo sin peers, confirmar de inmediato
            if not self.peers:
                self.commit_index = new_index
                self._apply_committed_entries()
                return {
                    "ok": True,
                    "committed": True,
                    "index": new_index,
                    "term": self.current_term,
                    "leaderId": self.node_id,
                }

        # Disparar replicación inmediata a seguidores y esperar mayoría
        self._replicate_to_all_peers()
        deadline = time.monotonic() + timeout
        with self.lock:
            while self.commit_index < new_index:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return {
                        "ok": False,
                        "committed": False,
                        "error": "COMMIT_TIMEOUT",
                        "index": new_index,
                        "leaderId": self.node_id,
                    }
                self._commit_cond.wait(timeout=remaining)

            return {
                "ok": True,
                "committed": True,
                "index": new_index,
                "term": self.current_term,
                "leaderId": self.node_id,
            }