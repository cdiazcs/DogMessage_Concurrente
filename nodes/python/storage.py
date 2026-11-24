"""Persistencia atómica y recuperación de estado para nodos de consenso."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional


class NodeStorage:
    def __init__(self, node_id: str, data_dir: str = "data") -> None:
        self.base_dir = Path(data_dir) / node_id
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.base_dir / "consensus_state.json"

    def load(self) -> Dict[str, Any]:
        if not self.state_file.exists():
            return {
                "currentTerm": 0,
                "votedFor": None,
                "log": [],  # Lista de dicts: {"index": int, "term": int, "requestId": str, "command": str}
            }
        try:
            with self.state_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return {
                "currentTerm": int(data.get("currentTerm", 0)),
                "votedFor": data.get("votedFor", None),
                "log": list(data.get("log", [])),
            }
        except (OSError, json.JSONDecodeError, ValueError):
            return {"currentTerm": 0, "votedFor": None, "log": []}

    def save(self, current_term: int, voted_for: Optional[str], log: List[Dict[str, Any]]) -> None:
        payload = {
            "currentTerm": current_term,
            "votedFor": voted_for,
            "log": log,
        }
        tmp_file = self.state_file.with_suffix(".tmp")
        with tmp_file.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, self.state_file)