# DM2 · Contrato TCP binario

**Fase I · Propuesta para revisión de los tres integrantes.** Implementación mediante SDK y sockets TCP en Java, Python y Kotlin. Los valores asignados están en el [registro de mensajes](message-types.md).

## Transporte y encabezado

Una conexión lleva una solicitud y, cuando corresponde, una respuesta. El servidor escribe la respuesta completa y cierra. El cliente lee una sola respuesta y cierra. No hay multiplexación ni reutilización. Leer exactamente 4 bytes de prefijo y después exactamente la longitud anunciada, aun si TCP fragmenta o agrupa bytes. Una segunda trama en la misma conexión nunca se procesa. El plazo de conexión es de 5 s. Cada lectura completa tiene un presupuesto de 5 s desde el primer intento de leer el prefijo, medido con reloj monotónico y aplicado también al cuerpo.

| Orden | Campo | Codificación |
|---|---|---|
| 1 | `bodyLength` | u32, cantidad de bytes posteriores al prefijo, de 1 a 1 048 576 |
| 2 | `magic` | 4 bytes ASCII `44 4d 32 00` |
| 3 | `algorithm` | u8 |
| 4 | `type` | u8 |
| 5 | `requestId` | 16 bytes opacos, distinto de todo ceros |
| 6 | `senderId` | NodeId |
| 7 | `payload` | Campos del tipo indicado hasta agotar el cuerpo |

El cuerpo DM2 tiene al menos 24 bytes. Todos los enteros son unsigned y big-endian. `u8`, `u16` y `u32` tienen 1, 2 y 4 bytes. `R64` ocupa 8 bytes y se restringe a 0…2⁶³−1 para representarse con `long`/`Long`; Python aplica el mismo rango. Toda suma de índices se valida contra el máximo antes de calcularla. No hay alineación, relleno, terminador ni serialización nativa.

- `NodeId` = u8 longitud (1…32) + bytes ASCII sensibles a mayúsculas de `[A-Za-z0-9_-]`. `OptionalNodeId` permite longitud 0 para ausente.
- `Bool` = u8 (0 falso, 1 verdadero).
- `Command` = u32 longitud (1…65 536) + bytes opacos. Los tres lenguajes conservan los bytes sin conversión a texto. El formato de aplicación queda fuera de esta fase.
- `LogPosition` = `index:R64, term:R64`. Ambos son cero para el origen ficticio o ambos positivos para una entrada real. Índices empiezan en 1, son consecutivos y el término de cada entrada es no decreciente. El término inicial de una réplica vacía es 0, y una elección comienza en un término positivo. Los contadores no pueden desbordar.
- `requestId` identifica la solicitud. El iniciador genera un valor aleatorio no nulo. En texto de diagnóstico son 32 dígitos hexadecimales en orden de transmisión. La respuesta repite los 16 bytes y el algoritmo e identifica al respondedor con su propio `senderId`. El iniciador verifica conexión, ID, algoritmo, tipo permitido y emisor esperado, además de la validez completa del payload, antes de usar una respuesta, incluido su término. Cada intento de RPC entre nodos usa un ID nuevo. El cliente reutiliza el mismo par `(senderId, requestId)` y los mismos bytes al reintentar un comando.

## Raft (algorithm 1)

Cada fila indica el orden exacto del payload. La identidad del candidato o líder está en el encabezado. El clúster usa membresía fija configurada fuera de banda. Las RPC entre réplicas y sus respuestas se aceptan únicamente de identificadores de réplica configurados.

| Mensaje | Payload |
|---|---|
| `RAFT_REQUEST_VOTE` | `term:R64, lastLog:LogPosition` |
| `RAFT_REQUEST_VOTE_RESPONSE` | `term:R64, voteGranted:Bool` |
| `RAFT_APPEND_ENTRIES` | `term:R64, prevLog:LogPosition, leaderCommit:R64, entryCount:u16, entries[entryCount]` |
| `RAFT_APPEND_ENTRIES_RESPONSE` | `term:R64, success:Bool, matchIndex:R64` |
| `CLIENT_COMMAND` | `command:Command` |
| `CLIENT_RESULT` | `status:u8, logIndex:R64, leaderHint:OptionalNodeId` |

Cada entrada comienza con `entryTerm:R64, entryKind:u8`. Para `entryKind=0` (NO_OP) termina ahí. Para `entryKind=1` (COMMAND) sigue `clientId:NodeId, clientRequestId:16 bytes, command:Command`; los 16 bytes del ID de solicitud no pueden ser todos cero. Los demás valores de `entryKind` son inválidos. Su índice implícito es `prevLog.index + posición`, contando desde 1. `entryCount` admite 0…256 y el conjunto debe caber en el límite de cuerpo. `entryTerm` es positivo, no decreciente desde `prevLog.term` y como máximo igual al término de la RPC. Los términos de las solicitudes Raft son positivos y los términos contenidos en sus posiciones no superan el término de la RPC. Los términos de respuestas son el `currentTerm` del respondedor después del procesamiento. Pueden valer 0 solo en una respuesta negativa. `leaderCommit` puede superar el último índice incluido en el lote.

### RequestVote

Una solicitud válida de término superior obliga a avanzar el término, limpiar el voto y pasar a seguidor, incluso si luego se deniega. Con término inferior se responde falso. Para conceder el voto en el término actual, el nodo debe tener el voto libre o registrado a favor del mismo candidato, y el par del candidato `(lastLog.term, lastLog.index)` debe ser lexicográficamente mayor o igual al local. Persistir los cambios de término y voto antes de responder. Conceder el voto reinicia el plazo de elección. El candidato cuenta una sola respuesta favorable por réplica en su elección vigente.

### AppendEntries

Una solicitud válida de término superior aplica la misma actualización persistente de término. Con término inferior se responde falso. Para término igual al actual, el candidato pasa a seguidor al recibir la RPC del líder. Una RPC válida de un líder de término vigente reinicia el plazo de elección incluso si su posición previa resulta incompatible. Comprobar la posición previa también para un heartbeat (`entryCount=0`). Ante término obsoleto o posición previa incompatible, devolver `success=0, matchIndex=0`; para un rechazo por incompatibilidad en el término vigente el líder reduce `nextIndex` hasta un mínimo de 1 y reintenta. Si coincide, conservar las entradas coincidentes y, a partir de la primera entrada nueva con término distinto en el mismo índice, eliminar el sufijo en conflicto e insertar las nuevas entradas. Conservar el sufijo local si no se detecta conflicto. Persistir las modificaciones antes de responder con `success=1, matchIndex=prevLog.index + entryCount`. Actualizar `commitIndex` local al máximo entre su valor anterior y `min(leaderCommit, matchIndex)`. Aplicar en orden solo lo comprometido.

Al asumir el liderazgo se agrega una entrada NO_OP del término actual para poder comprometer entradas de términos previos incluso sin tráfico de clientes. El líder valida el `matchIndex` de éxito contra el final exacto del lote enviado. Mantiene un intento activo por seguidor y descarta respuestas de intentos vencidos o estados electorales anteriores. Un término superior en una respuesta válida provoca el paso a seguidor y la persistencia del nuevo término. El líder solo avanza el compromiso por su propia cuenta hasta un índice de su término confirmado en una mayoría.

### Comandos y resultados

| Estado | Valor | `logIndex` | `leaderHint` |
|---|---|---|---|
| `COMMITTED` | 0 | índice positivo confirmado | ausente |
| `NOT_LEADER` | 1 | 0 | réplica conocida o ausente |
| `RETRY` | 2 | 0 | ausente |
| `REQUEST_ID_CONFLICT` | 3 | 0 | ausente |

El líder devuelve `COMMITTED` solo tras confirmar el comando. Si termina la espera sin confirmación puede responder `RETRY`. Un timeout o `RETRY` deja el resultado indeterminado. Para cada par de cliente, el líder reutiliza el índice de un comando coincidente ya registrado (pendiente o comprometido); bytes distintos para un par ya registrado producen `REQUEST_ID_CONFLICT`. Reconstruir el reconocimiento de solicitudes desde el log al recuperar o cambiar de líder, y deduplicar al aplicar. Una entrada pendiente eliminada por reparación deja de reservar ese par. Una respuesta `NOT_LEADER` puede orientar un reintento a una réplica configurada. Una entrada NO_OP solo avanza la posición aplicada. Los diagramas muestran los [intercambios previstos](diagramas-raft.md).

## Rechazos y orden de validación

1. Si la longitud externa es inválida, hay EOF prematuro, vence el plazo de lectura o la firma no se reconoce, cerrar sin respuesta DM2. El prefijo se valida antes de reservar memoria. La excepción por DM1 se detalla abajo.
2. Para firma DM2 válida, cerrar en silencio ante encabezado incompleto o inválido, ID nulo o identificador inválido. Cerrar en silencio si el tipo es 0, si tiene el bit alto activo (respuesta) o si es `ERROR`. El servidor solo procesa solicitudes. Un iniciador que reciba una respuesta inválida cierra y marca el intento fallido.
3. Con encabezado válido de solicitud, comprobar algoritmo soportado, luego tipo asignado en ese algoritmo, luego todos los campos del payload (incluye longitudes internas, rangos, valores enumerados, posición y consumo exacto) y finalmente la membresía del emisor si aplica. El primer fallo genera respectivamente `UNSUPPORTED_ALGORITHM`, `UNKNOWN_TYPE`, `MALFORMED_PAYLOAD` o `UNKNOWN_MEMBER`. El cuerpo debe estar completo incluso para devolver un error.
4. Los fallos de formato, membresía o correspondencia no cambian estado de consenso. Un éxito de AppendEntries puede llevar índice 0 para un heartbeat en el origen; un fallo exige índice 0. Un término inferior al de la solicitud puede decodificarse en una respuesta negativa, pero el iniciador descarta esa respuesta en una RPC activa: una réplica conforme ya habría avanzado su término. Término obsoleto, voto denegado o log previo incompatible se expresan mediante la respuesta Raft normal. Si falla la persistencia necesaria, cerrar la conexión sin emitir un éxito ni un rechazo Raft basado en estado incierto.

`ERROR` lleva solo `code:u8` y usa el algoritmo de la solicitud aunque sea desconocido. Un error se considera una respuesta correlacionada al intento, sin término ni efecto sobre consenso. Los valores se definen en el registro.

## Coexistencia con DM1 y segundo algoritmo

El listener mixto conserva el prefijo de longitud. Un cuerpo que empieza con `DM1|` se entrega al [contrato DM1](CONTRATO_TCP_v1.md), incluyendo PING/PONG y su error. Un cuerpo que empieza con `DM2\0` se procesa como binario. Cualquier otro cuerpo se cierra según las reglas anteriores. Un nodo DM1 heredado puede devolver su error DM1 o cerrar al recibir DM2. Elegir DM1 o DM2 por capacidad configurada del destino. Una respuesta DM1 a un intento DM2 cuenta como respuesta inválida, sin reinterpretación ni reintento de consenso en DM1. DM1 sigue disponible para diagnóstico durante la migración.

El identificador de algoritmo separa espacios de tipos. El valor 2 queda reservado para el algoritmo asignado por el docente; mientras no tenga especificación aprobada se rechaza como no soportado. Su diseño podrá reutilizar framing, correlación y códigos de error y definir sus propios mensajes y semántica. Los cambios incompatibles al encabezado requieren una firma nueva. Los payloads DM2 existentes se conservan byte a byte cuando se amplía el registro.

Ver [fixtures DM2](fixtures/dm2.json) y [plan de interoperabilidad](PLAN_INTEROPERABILIDAD_DM2.md).

Las reglas de elección y replicación se basan en la figura 2 y las secciones 5 y 8 del [artículo de Raft de Ongaro y Ousterhout](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-extended.pdf).
