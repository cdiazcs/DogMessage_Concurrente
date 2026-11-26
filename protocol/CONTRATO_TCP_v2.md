# DM2 · Contrato TCP binario

Protocolo común entre los nodos Java, Python y Kotlin. Casos de prueba en
[fixtures/](fixtures/).

## Trama

Cada conexión lleva una solicitud y, como máximo, una respuesta. Luego se cierra.

| Campo | Codificación |
|---|---|
| `bodyLength` | u32, de 1 a 1 048 576 bytes |
| `magic` | `44 4d 32 00` (`DM2\0`) |
| `algorithm` | u8: 1 Raft, 2 2PC, 3 feed de chat |
| `type` | u8; las respuestas tienen el bit alto activo (`0x80`) |
| `requestId` | 16 bytes aleatorios, distinto de cero |
| `senderId` | u8 longitud (1–32) + ASCII `[A-Za-z0-9_-]` |
| `payload` | Campos del tipo, hasta el final del cuerpo |

Los enteros son big-endian y no tienen signo. `R64` es un u64 limitado a 2⁶³−1.
`Command` = u32 longitud (1–65 536) + bytes opacos. Plazos: 5 s para conectar y 5 s
para leer la trama completa.

## Raft (algorithm 1)

| Tipo | Mensaje | Payload |
|---|---|---|
| `0x01` | RequestVote | `term:R64, lastIndex:R64, lastTerm:R64` |
| `0x81` | VoteResponse | `term:R64, granted:u8` |
| `0x02` | AppendEntries | `term:R64, prevIndex:R64, prevTerm:R64, leaderCommit:R64, count:u16, entries` |
| `0x82` | AppendResponse | `term:R64, success:u8, matchIndex:R64` |
| `0x03` | ClientCommand | `command:Command` |
| `0x83` | ClientResult | `status:u8, logIndex:R64, leaderHint:NodeId opcional` |

Cada entrada es `term:R64, kind:u8` (0 = NO_OP; 1 = COMMAND seguido de
`clientId:NodeId, clientRequestId:16 bytes, command:Command`). Se aceptan hasta 256
entradas por lote. Estados de `ClientResult`: 0 `COMMITTED`, 1 `NOT_LEADER`,
2 `RETRY`, 3 `REQUEST_ID_CONFLICT`.

Las reglas de elección y replicación siguen la figura 2 del
[artículo de Raft](https://raft.github.io/raft.pdf). Término y voto se guardan en disco
antes de responder. El líder agrega un NO_OP al asumir el cargo y deduplica los
comandos por `(clientId, clientRequestId)`.

## 2PC (algorithm 2)

El coordinador es fijo. `Tx` = `sequence:R64, xid:16 bytes, coordinator:NodeId,
client:NodeId, requestId:16 bytes, command:Command`.

| Tipo | Mensaje | Payload |
|---|---|---|
| `0x01` / `0x81` | Submit / resultado | `command` / `status:u8, sequence:R64, leader:NodeId` |
| `0x02` / `0x82` | Prepare / voto | `Tx` / `sequence, xid, vote:u8` |
| `0x03` / `0x83` | Commit / ack | `Tx` / `sequence, xid, ok:u8` |
| `0x04` / `0x84` | Abort / ack | `Tx` / `sequence, xid, ok:u8` |
| `0x05` / `0x85` | Query / decisión | `sequence, xid` / `sequence, xid, decision:u8` |

Estados de Submit: 0 `COMMITTED`, 1 `ABORTED`, 2 `PENDING`, 3 `CONFLICT`,
4 `NOT_COORDINATOR`. Decisión de Query: 0 desconocida, 1 commit, 2 abort. Un
participante preparado que pierde al coordinador queda bloqueado hasta conocer la
decisión.

## Feed de chat (algorithm 3)

El gateway lee las operaciones aplicadas con `type 0x01`: `after:R64, hmac:32 bytes`.
La firma es HMAC-SHA256 con `DOG_CHAT_KEY` sobre `"DM2-chat-feed-v1" + requestId + after`.
La respuesta `0x81` es `cursor:R64, count:u16` seguida de `position:R64, length:u32, bytes`,
con un máximo de 64 entradas `CHAT1`.

## Errores

`ERROR` (`0xFF`) lleva `code:u8`: 1 `UNSUPPORTED_ALGORITHM`, 2 `UNKNOWN_TYPE`,
3 `MALFORMED_PAYLOAD`, 4 `UNKNOWN_MEMBER`. Si la trama es inválida, se cierra la
conexión sin responder.

## Otros protocolos

- **DOG/1 (clientes ↔ gateway):** JSON `{v, id, op, data, session}` con prefijo u32
  sobre TLS (TCP 9443) o HTTPS (8443). Las operaciones están en `gateway/chat_gateway.py`.
- **DMB1 (adjuntos):** bloques de 8 KiB, archivos de hasta 50 MiB, verificados con
  SHA-256 y con una prueba HMAC de durabilidad (`DOG_MEDIA_KEY`).
- **E1 (cifrado punto a punto):** claves P-256 (ECDH + ECDSA) por dispositivo,
  HKDF-SHA256 y AES-256-GCM. El gateway y los nodos solo ven sobres cifrados.
