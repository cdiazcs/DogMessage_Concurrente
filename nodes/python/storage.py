"""Persistencia atómica del estado Raft (término, voto, commitIndex y log tipado).

Formato JSON versionado. Los bytes de cada comando se guardan en base64 y el requestId en hex,
así el log conserva exactamente lo que viajó en DM2. Cada escritura va a un temporal en el mismo
directorio, se sincroniza con fsync y se publica con os.replace (atómico en POSIX y Windows).
En POSIX además se sincroniza el directorio; en Windows no es posible abrir un directorio para fsync.
"""
from __future__ import annotations

import base64
import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import dm2_codec as dm2

FORMAT = "dog-raft-state/1"
STATE_FILE = "raft_state.json"


class StorageError(Exception):
    """Estado persistido ilegible o incompatible: el nodo no debe arrancar con él."""


@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: Optional[str] = None
    commit_index: int = 0
    log: List[dm2.LogEntry] = field(default_factory=list)


def _entry_to_json(index: int, entry: dm2.LogEntry) -> dict:
    data = {"index": index, "term": entry.term, "kind": entry.kind}
    if entry.kind == dm2.COMMAND:
        data.update(clientId=entry.client_id, clientRequestId=entry.client_request_id.hex(),
                    command=base64.b64encode(entry.command).decode("ascii"))
    return data


def _entry_from_json(data: dict) -> dm2.LogEntry:
    if data["kind"] == dm2.COMMAND:
        return dm2.LogEntry.of_command(int(data["term"]), data["clientId"], bytes.fromhex(data["clientRequestId"]),
                                       base64.b64decode(data["command"], validate=True))
    return dm2.LogEntry(int(data["term"]), int(data["kind"]))


class NodeStorage:
    def __init__(self, directory: Path, node_id: str) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.node_id = node_id
        self.state_file = self.directory / STATE_FILE
        for stale in self.directory.glob("raft_state-*.tmp"):
            try:
                stale.unlink()  # Temporal de una escritura interrumpida: nunca se publicó.
            except OSError:
                pass

    def load(self) -> PersistentState:
        if not self.state_file.exists():
            return PersistentState()
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
            if data.get("format") != FORMAT or data.get("nodeId") != self.node_id:
                raise StorageError("Estado Raft de otro formato u otro nodo")
            state = PersistentState(int(data["currentTerm"]), data["votedFor"], int(data["commitIndex"]),
                                    [_entry_from_json(item) for item in data["log"]])
            for position, item in enumerate(data["log"], start=1):
                if int(item["index"]) != position:
                    raise StorageError("Índices del log no consecutivos")
            previous = 0
            for entry in state.log:
                if entry.term < previous:
                    raise StorageError("Términos del log decrecientes")
                previous = entry.term
            if state.current_term < previous or not 0 <= state.commit_index <= len(state.log):
                raise StorageError("Término o commitIndex incompatibles con el log")
            if state.voted_for is not None and not dm2.valid_node_id(state.voted_for):
                raise StorageError("votedFor inválido")
            return state
        except StorageError:
            raise
        except (OSError, ValueError, KeyError, TypeError) as error:
            # Nunca arrancar "en blanco": perder el voto permitiría votar dos veces en un término.
            raise StorageError(f"No se pudo leer {self.state_file}: {error}") from error

    def save(self, state: PersistentState) -> None:
        payload = {
            "format": FORMAT,
            "nodeId": self.node_id,
            "currentTerm": state.current_term,
            "votedFor": state.voted_for,
            "commitIndex": state.commit_index,
            "log": [_entry_to_json(i, e) for i, e in enumerate(state.log, start=1)],
        }
        descriptor, temporary = tempfile.mkstemp(prefix="raft_state-", suffix=".tmp", dir=self.directory)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.state_file)
        except BaseException:
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise
        if os.name != "nt":
            directory = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
