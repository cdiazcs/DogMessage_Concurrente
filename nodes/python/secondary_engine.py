"""Segundo Algoritmo de Consenso (Two-Phase Commit / 2PC) bajo la interfaz ConsensusEngine."""
from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from protocol_dm2 import build_dm2
from storage import NodeStorage


class TwoPhaseCommitEngine:
    def __init__(
        self,
        node_id: str,
        peers: Dict[str, Tuple[str, int]],
        rpc_sender: Callable[[str, Tuple[str, int], str, float], Optional[Dict[str, Any]]],
        data_dir: str = "data",
    ) -> None:
        self.node_id = node_id
        self.peers = peers
        self.rpc_sender = rpc_sender
        self.storage = NodeStorage(node_id, data_dir=f"{data_dir}_2pc")

        self.lock = threading.RLock()
        persisted = self.storage.load()
        self.epoch: int = persisted["currentTerm"]
        self.log: List[Dict[str, Any]] = persisted["log"]
        self.prepared_tx: Dict[str, Dict[str, Any]] = {}
        self.applied_requests: Dict[str, Dict[str, Any]] = {
            e["requestId"]: e for e in self.log if "requestId" in e
        }

        self.isolated: bool = False
        self.artificial_delay_ms: int = 0
        self._metrics: Dict[str, Any] = {
            "algorithm": "2PC",
            "tx_prepared": 0,
            "tx_committed": 0,
            "tx_aborted": 0,
            "rpc_sent": 0,
            "rpc_received": 0,
        }

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def set_fault_state(self, isolated: Optional[bool] = None, delay_ms: Optional[int] = None) -> Dict[str, Any]:
        with self.lock:
            if isolated is not None:
                self.isolated = bool(isolated)
            if delay_ms is not None:
                self.artificial_delay_ms = max(0, int(delay_ms))
            return {"isolated": self.isolated, "delay_ms": self.artificial_delay_ms}

    def leader(self) -> Optional[str]:
        # En 2PC el coordinador puede ser el nodo que recibe la transacción
        return self.node_id

    def committedEntries(self) -> List[Dict[str, Any]]:
        with self.lock:
            return list(self.log)

    def metrics(self) -> Dict[str, Any]:
        with self.lock:
            return {
                **self._metrics,
                "nodeId": self.node_id,
                "state": "COORDINATOR",
                "term": self.epoch,
                "leaderId": self.node_id,
                "logLength": len(self.log),
                "commitIndex": len(self.log),
                "isolated": self.isolated,
            }

    def propose(self, command: str, request_id: str, timeout: float = 3.0) -> Dict[str, Any]:
        with self.lock:
            if self.isolated:
                return {"ok": False, "error": "NODE_ISOLATED"}
            if request_id in self.applied_requests:
                entry = self.applied_requests[request_id]
                return {"ok": True, "committed": True, "duplicate": True, "index": entry["index"]}

            self.epoch += 1
            tx_index = len(self.log) + 1
            tx_entry = {
                "index": tx_index,
                "term": self.epoch,
                "requestId": request_id,
                "command": command,
            }
            self.prepared_tx[request_id] = tx_entry
            self._metrics["tx_prepared"] += 1
            peers_snapshot = dict(self.peers)

        # Fase 1: PREPARE en todos los participantes (2PC requiere unanimidad o quórum completo)
        votes_yes = 1
        for pid, addr in peers_snapshot.items():
            with self.lock:
                self._metrics["rpc_sent"] += 1
            msg = build_dm2("CONSENSUS2_PREPARE", self.node_id, {"entry": tx_entry})
            resp = self.rpc_sender(pid, addr, msg, timeout / 2)
            if resp and resp.get("payload", {}).get("vote") == "YES":
                with self.lock:
                    self._metrics["rpc_received"] += 1
                votes_yes += 1

        all_ready = votes_yes == (len(peers_snapshot) + 1)

        # Fase 2: COMMIT o ABORT
        decision = "COMMIT" if all_ready else "ABORT"
        for pid, addr in peers_snapshot.items():
            with self.lock:
                self._metrics["rpc_sent"] += 1
            msg = build_dm2(
                "CONSENSUS2_COMMIT",
                self.node_id,
                {"requestId": request_id, "decision": decision, "entry": tx_entry},
            )
            self.rpc_sender(pid, addr, msg, timeout / 2)

        with self.lock:
            self.prepared_tx.pop(request_id, None)
            if all_ready:
                self.log.append(tx_entry)
                self.applied_requests[request_id] = tx_entry
                self.storage.save(self.epoch, self.node_id, self.log)
                self._metrics["tx_committed"] += 1
                return {
                    "ok": True,
                    "committed": True,
                    "algorithm": "2PC",
                    "index": tx_index,
                    "leaderId": self.node_id,
                }
            self._metrics["tx_aborted"] += 1
            return {
                "ok": False,
                "committed": False,
                "algorithm": "2PC",
                "error": "2PC_ABORTED_MISSING_PARTICIPANT",
            }

    def handle_prepare(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self.lock:
            if self.isolated:
                return {"vote": "NO", "reason": "ISOLATED"}
            if self.artificial_delay_ms > 0:
                time.sleep(self.artificial_delay_ms / 1000.0)
            self._metrics["rpc_received"] += 1
            entry = payload.get("entry", {})
            req_id = entry.get("requestId")
            if not req_id:
                return {"vote": "NO"}
            self.prepared_tx[req_id] = entry
            self._metrics["tx_prepared"] += 1
            return {"vote": "YES"}

    def handle_commit(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self.lock:
            if self.isolated:
                return {"ack": False}
            self._metrics["rpc_received"] += 1
            req_id = payload.get("requestId")
            decision = payload.get("decision")
            entry = self.prepared_tx.pop(req_id, payload.get("entry"))
            if decision == "COMMIT" and entry and req_id not in self.applied_requests:
                self.log.append(entry)
                self.applied_requests[req_id] = entry
                self.storage.save(self.epoch, self.node_id, self.log)
                self._metrics["tx_committed"] += 1
            elif decision == "ABORT":
                self._metrics["tx_aborted"] += 1
            return {"ack": True}