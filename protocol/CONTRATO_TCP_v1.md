# DM1 — Contrato TCP común (implementado en Java / Python / Kotlin)

**Versión:** `DM1` · **Transporte:** TCP puro · **Estado:** conectividad de arranque, no consenso.

## Framing de bytes

Cada mensaje consta de:

1. **4 bytes** con longitud unsigned de 32 bits **big-endian** del cuerpo en bytes.
2. **N bytes** de carga UTF-8 (`1 <= N <= 1,048,576`).

No usar `readLine` ni asumir que `recv` devuelve la trama completa. Leer exactamente 4 bytes y luego exactamente N bytes. Nunca interpretar el framing como un mensaje Raft.

## Operaciones v1 (únicas implementadas)

Cuerpo del PING:

```text
DM1|PING|<senderId>|<requestId>
```

Respuesta:

```text
DM1|PONG|<serverNodeId>|<sameRequestId>
```

Error de petición:

```text
DM1|ERROR|<serverNodeId>|00000000000000000000000000000000
```

- `senderId`, `serverNodeId`: 1-32 letras ASCII, números, `_` o `-`.
- `requestId`: **32 caracteres hexadecimales** (UUID aleatorio sin guiones). Es utilizado para comprobar correspondencia entre petición y respuesta.
- Delimitador: `|`, nunca permitido en identificadores.
- Tras una petición se entrega una respuesta y se cierra esa conexión de prueba.
- `PING` no se confirma por Raft. Solo mide conectividad.
- Límite de espera de I/O de cada conexión de prueba: 5 segundos.

## Casos comunes

```text
Cuerpo solicitud: DM1|PING|n2|0123456789abcdef0123456789abcdef
Cuerpo respuesta: DM1|PONG|n1|0123456789abcdef0123456789abcdef
```

Ver `fixtures/` para valores de referencia.

## Regla de compatibilidad

Una actualización de contrato se revisa por los **tres integrantes** y debe tener pruebas de lectura/escritura en Java, Python y Kotlin. No editar unilateralmente el wire format. Para cambios incompatibles, crear `DM2` y mantener la compatibilidad durante la migración.

## Futuro: mensajes de consenso (NO implementados)

Antes de codificar Raft, definir un contrato v2 con campos binarios de longitud explícita (evitar serialización dependiente del lenguaje) para:

- `RAFT_REQUEST_VOTE`, `RAFT_REQUEST_VOTE_RESPONSE`
- `RAFT_APPEND_ENTRIES`, `RAFT_APPEND_ENTRIES_RESPONSE`
- `CLIENT_COMMAND`, `CLIENT_RESULT`
- Identificadores, índice, term, prevLogIndex, prevLogTerm, commitIndex, longitud de entradas y resultado de éxito/fallo.

Las decisiones de seguridad, persistencia y reintento están pendientes; ver `docs/04_DISENO_CONSENSO.md`.
