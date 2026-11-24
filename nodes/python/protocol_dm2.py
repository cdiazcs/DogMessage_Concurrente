"""Contrato de mensajes DM2 (JSON sobre framing TCP de 4 bytes big-endian)."""
from __future__ import annotations

import json
from typing import Any, Dict

VALID_DM2_TYPES = {
    "RAFT_REQUEST_VOTE",
    "RAFT_REQUEST_VOTE_RESPONSE",
    "RAFT_APPEND_ENTRIES",
    "RAFT_APPEND_ENTRIES_RESPONSE",
    "CONSENSUS2_PREPARE",
    "CONSENSUS2_PREPARE_RESPONSE",
    "CONSENSUS2_COMMIT",
    "CONSENSUS2_COMMIT_RESPONSE",
    "CLIENT_COMMAND",
    "CLIENT_RESULT",
    "NODE_STATUS",
    "NODE_STATUS_RESPONSE",
    "FAULT_CONTROL",
    "FAULT_CONTROL_RESPONSE",
}


def build_dm2(msg_type: str, sender_id: str, payload: Dict[str, Any]) -> str:
    if msg_type not in VALID_DM2_TYPES:
        raise ValueError(f"Tipo de mensaje DM2 desconocido: {msg_type}")
    envelope = {
        "protocol": "DM2",
        "type": msg_type,
        "senderId": sender_id,
        "payload": payload,
    }
    return json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)


def parse_dm2(raw: str) -> Dict[str, Any]:
    data = json.loads(raw)
    if not isinstance(data, dict) or data.get("protocol") != "DM2":
        raise ValueError("No es una trama DM2 válida")
    msg_type = data.get("type")
    if msg_type not in VALID_DM2_TYPES:
        raise ValueError(f"Tipo DM2 no soportado: {msg_type}")
    if not isinstance(data.get("senderId"), str) or not isinstance(data.get("payload"), dict):
        raise ValueError("Campos senderId o payload inválidos")
    return data