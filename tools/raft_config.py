"""Configuración de red compartida por las herramientas Raft."""
from __future__ import annotations

from dataclasses import dataclass, replace
import json
from pathlib import Path

from cluster_lib import dm2


@dataclass(frozen=True)
class NodeAddress:
    id: str
    language: str
    host: str
    port: int
    data_dir: str | None = None
    coordinator: str | None = None

    @property
    def endpoint(self):
        return self.host, self.port


def load_config(path: Path, algorithm="raft") -> dict[str, NodeAddress]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if (not isinstance(document, dict) or document.get("algorithm") not in ("raft", "2pc")
            or (algorithm is not None and document.get("algorithm") != algorithm)):
        raise ValueError(f"algorithm debe ser {algorithm or 'raft o 2pc'}")
    members, nodes = document.get("members"), document.get("nodes")
    if not isinstance(members, list) or not members or not all(dm2.valid_node_id(m) for m in members):
        raise ValueError("members debe contener identificadores DM2 válidos")
    if len(members) != len(set(members)) or not isinstance(nodes, list):
        raise ValueError("members duplicados o nodes inválido")

    addresses = {}
    endpoints = set()
    for node in nodes:
        if not isinstance(node, dict):
            raise ValueError("Cada elemento de nodes debe ser un objeto")
        id, lang, host, port, data = (node.get(k) for k in ("id", "language", "host", "port", "data_dir"))
        if not dm2.valid_node_id(id) or id in addresses:
            raise ValueError(f"Identificador ausente, inválido o repetido: {id!r}")
        if lang not in ("java", "python", "kotlin"):
            raise ValueError(f"{id}: lenguaje inválido")
        if (not isinstance(host, str) or not host or any(c.isspace() for c in host)
                or host in {"0.0.0.0", "::", "[::]"} or type(port) is not int or not 1 <= port <= 65535):
            raise ValueError(f"{id}: host o puerto inválido")
        endpoint = (host.casefold(), port)
        if endpoint in endpoints:
            raise ValueError(f"{id}: dirección repetida")
        if data is not None and (not isinstance(data, str) or not data.strip()):
            raise ValueError(f"{id}: data_dir inválido")
        endpoints.add(endpoint)
        addresses[id] = NodeAddress(id, lang, host, port, data)
    if set(addresses) != set(members):
        raise ValueError("members debe coincidir con los IDs de nodes")
    coordinator = document.get("coordinator")
    if document["algorithm"] == "2pc":
        if not dm2.valid_node_id(coordinator) or coordinator not in addresses:
            raise ValueError("coordinator debe pertenecer a members")
        return {id: replace(addresses[id], coordinator=coordinator) for id in members}
    return {id: addresses[id] for id in members}
