# DM2 · Registro de algoritmos y mensajes

Los campos y reglas normativas están en el [contrato DM2](CONTRATO_TCP_v2.md). Los identificadores ocupan un byte cada uno.

| Algoritmo | Valor | Estado |
|---|---|---|
| Reservado | `0x00` | No soportado |
| Raft | `0x01` | Definido en fase I |
| Algoritmo asignado por el docente | `0x02` | Reservado, no soportado |
| Futuras asignaciones | `0x03–0xff` | No soportado |

| Algoritmo | Tipo | Valor | Dirección y respuesta prevista |
|---|---|---|---|
| Raft | `RAFT_REQUEST_VOTE` | `0x01` | Candidato a réplica → `0x81` |
| Raft | `RAFT_REQUEST_VOTE_RESPONSE` | `0x81` | Réplica a candidato |
| Raft | `RAFT_APPEND_ENTRIES` | `0x02` | Líder a réplica → `0x82` |
| Raft | `RAFT_APPEND_ENTRIES_RESPONSE` | `0x82` | Réplica a líder |
| Raft | `CLIENT_COMMAND` | `0x03` | Cliente a réplica → `0x83` |
| Raft | `CLIENT_RESULT` | `0x83` | Réplica a cliente |
| Cualquiera | `ERROR` | `0xff` | Respuesta alternativa a una solicitud |

Los valores `0x01–0x7f` quedan disponibles para solicitudes dentro del algoritmo. Los valores `0x80–0xfe` quedan disponibles para respuestas. El tipo `0x00` está reservado. Un tipo sin asignar no se reutiliza implícitamente desde otro algoritmo.

| Código de `ERROR` | Valor | Causa |
|---|---|---|
| `UNSUPPORTED_ALGORITHM` | 1 | Algoritmo sin soporte |
| `UNKNOWN_TYPE` | 2 | Solicitud sin tipo asignado en el algoritmo |
| `MALFORMED_PAYLOAD` | 3 | Payload inválido para el tipo |
| `UNKNOWN_MEMBER` | 4 | Emisor de una RPC Raft no configurado como réplica |

Todo código de error distinto de 1…4 invalida la respuesta DM2 de fase I.
