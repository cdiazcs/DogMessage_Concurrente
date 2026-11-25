# Lista de requisitos y evidencia — PC4 2026-II

**Fuente:** `P4cc4P1-262_v04.pdf`, pp. 1-5. El enunciado **no indica puntajes por criterio**. No atribuir ponderaciones inventadas. Estado inicial: TODO salvo conectividad.

| Requisito | Estado v0.1 | Prueba/evidencia que debe existir al entregar | PR |
|---|---|---|---|
| 3 nodos en computadoras distintas LAN/WiFi | PARCIAL: ejecutables; LAN por comprobar | Video 3 equipos + puertos/IP + pruebas de comunicación | — |
| LP1, LP2, LP3, solo SDK y sockets/hilos | PARCIAL: Java, Python, Kotlin TCP | Código comentado y build de tres nodos | — |
| Raft desde cero | PARCIAL: Java (LP1) y Python (LP2) desde cero, interoperables en DM2 binario; clúster mixto n1 Java + n2/n3 Python en loopback con líder Java y con líder Python (`tools/mixed_cluster_test.py`, 5/5 corridas PASS). Kotlin (LP3) pendiente | Kotlin en el clúster y pruebas LAN | fase I |
| Segundo algoritmo asignado desde cero | PENDIENTE | Implementación en 3 nodos + comparación con Raft | — |
| Comunicación cliente desktop/móvil | PENDIENTE | Mensajes en ambos sentidos | — |
| Versión web (objetivo general) | POR DEFINIR | Confirmar alcance con docente | — |
| Chat privado/grupal y orden consistente | PENDIENTE | 3 usuarios y orden compartido | — |
| Imágenes y archivos | PENDIENTE | Hash de integridad + recuperación de contenido | — |
| Clonación mediante QR | PENDIENTE | Sincronización de historial anterior y autorización | — |
| Cifrado de extremo a extremo | PENDIENTE | Prueba cliente-cliente y servidor sin texto legible | — |
| Fallo del líder / nueva elección | PARCIAL: pruebas automáticas en loopback con 3 procesos Java, con 3 Python (`fault_simulator.py`) y mixtas Java+Python (`mixed_cluster_test.py`, paso 4) | Logs con término/líder y demo en vivo en LAN | fase I |
| Caída de seguidor / recuperación | PARCIAL: nodo revivido se pone al día y `--inspect` de Java y Python muestra entradas comprometidas idénticas (índice, término, tipo, clientId, clientRequestId, command) en loopback | Repetir en LAN | fase I |
| Partición de red | PARCIAL: líder Python aislado (`fault.json`), la mayoría reelige y confirma; al sanar el aislado se sincroniza (`fault_simulator.py`, escenarios 2-3) | Partición real en LAN con Java y explicación mayoría/minoría | fase I |
| Métricas de latencia y throughput | PENDIENTE | CSV, método de medición y comparación | — |
| Reportes automáticos | PENDIENTE | Script que produzca archivo y gráfico sin inventar datos | — |
| Arquitectura y diagramas de protocolo | PARCIAL: [diseño DM2 propuesto](../protocol/CONTRATO_TCP_v2.md) y [diagramas Raft](../protocol/diagramas-raft.md) | Validar los intercambios Raft implementados y documentar el segundo algoritmo | — |
| Informe PDF | PENDIENTE | Informe coherente con lo realmente implementado | — |
| Presentación PDF + demo | PENDIENTE | Diapositivas, secuencia de demo y pruebas en vivo | — |
| ZIP final Univirtual sin temporales | PENDIENTE | Fuentes + informe/presentación PDF según enunciado | — |

**Meta:** cerrar cada pendiente mediante PR, test, evidencia y actualización de esta tabla. No marcar como listo sin ejecutar la demostración.

## Evidencia Java de fase II

El nodo Java incorpora transporte TCP concurrente, framing mixto DM1/DM2 y
validación de RPC con el JDK. La prueba local usa manejadores simulados y los
fixtures compartidos para verificar codificación, rechazo y correlación. En modo de
transporte, el proceso `NodeMain` cierra las solicitudes DM2 válidas sin
respuesta cuando requieren un manejador de consenso o de aplicación. Las tramas inválidas
siguen las reglas de rechazo del contrato.

```bash
out=$(mktemp -d)
javac --release 17 -Xlint:all -Werror -d "$out" \
  nodes/java/src/dogmessenger/NodeMain.java \
  nodes/java/test/dogmessenger/TransportTest.java
java -cp "$out" dogmessenger.TransportTest
python3 tools/smoke_test.py
```

El script Ubuntu configura `n1,n2,n3` como réplicas. En un arranque manual
`--members n1,n2,n3` activa esa lista y, si se omite, solo se configura el ID
local. La ejecución local verifica DM2 en Java y repite los PING DM1 de los tres
lenguajes. La matriz DM2 entre lenguajes y la LAN están pendientes.

## Evidencia Java de fase III

[Configuración y pruebas de elección Raft](06_RAFT_JAVA.md). La prueba con tres
procesos Java en loopback cubre elección por mayoría, heartbeats, caída del
líder, reelección y reinicio con estado persistido. La replicación de
entradas, el compromiso y las pruebas de clúster en LAN quedan pendientes.

## Evidencia Java de fase IV-A

[Diseño y pruebas del log local](06_RAFT_JAVA.md#log-local-de-fase-iv-a).
La suite de log verifica escrituras concurrentes, reapertura, colas incompletas
y detección de corrupción. La prueba de caída del líder verifica que su
entrada local `NO_OP` sobreviva a la terminación abrupta del proceso.

## Evidencia Java de fase IV-B

[Diseño y pruebas de replicación](06_RAFT_JAVA.md#replicación-de-fase-iv-b).
La prueba de tres procesos en loopback comprueba el retroceso de
`nextIndex`, la reparación de un sufijo divergente y la recuperación
tras desconexión.

## Evidencia Java de fase IV-C

[Compromiso y recuperación](06_RAFT_JAVA.md#compromiso-y-recuperación-de-fase-iv-c).
Las pruebas cubren compromiso con dos de tres, ausencia de nuevas
confirmaciones al perder mayoría, caída del líder después de confirmar,
reconciliación, recuperación del índice y aplicación única por solicitud.
La integración DM2 trilingüe, la LAN y las mediciones de rendimiento
del clúster quedan pendientes.

## Raft Python sobre DM2 binario — fase I (sub-paso I.3)

El motor del integrante de Python (`nodes/python/raft_engine.py`, `storage.py`) ya habla DM2
binario mediante `nodes/python/dm2_codec.py` y `nodes/python/node.py`. Su estructura original
se conservó: en la fase I solo se cambió lo que el contrato DM2 o la interoperabilidad con Java
exigían.

```bash
python3 -m unittest tests.test_raft_python -v   # Windows: py -3 -m unittest tests.test_raft_python -v
python3 tools/fault_simulator.py                # Windows: py -3 tools\fault_simulator.py
```

`fault_simulator.py` conserva los cuatro escenarios originales con 3 nodos Python: elección y 5
comandos; partición del líder (aislamiento local mediante `<data-dir>/fault.json`, porque DM2 no
define mensajes de control), reelección y 5 comandos; sanación y sincronización; caída abrupta
del nuevo líder, reinicio desde disco y comparación con `--inspect` de las entradas comprometidas
de los 3 nodos.

### Corregido en la fase I porque bloqueaba la interoperabilidad o violaba el contrato DM2

| Problema original | Por qué bloqueaba | Corrección | Test |
|---|---|---|---|
| Éxito de AppendEntries con `matchIndex=len(log)` | Java descarta todo éxito cuyo `matchIndex` difiera de `prevLog.index + entryCount`: un seguidor Python nunca avanzaba | `matchIndex = prevLog.index + entryCount` | `test_match_index_is_end_of_batch_not_log_length` |
| Seguidor con `commitIndex = min(leaderCommit, len(log))` | Podía comprometer un sufijo local no verificado; el contrato exige `min(leaderCommit, matchIndex)` | Se usa `matchIndex` y el commit solo aumenta | ídem |
| `commit_index = len(log)` al reiniciar | Daba por comprometidas entradas pendientes; `--inspect` difería entre nodos | `commitIndex` se persiste y se recupera de disco | `test_commit_index_survives_restart` |
| Rechazo con `matchIndex` ≠ 0 | Respuesta inválida en DM2 | Fallo siempre con `matchIndex=0` | `test_heartbeat_origin_and_mismatch` |
| Lotes sin límite de entradas | DM2 admite 0…256 entradas y 1 MiB de cuerpo | Lote acotado igual que `RaftLog.batch` de Java | `test_leader_batches_are_bounded` |
| Reenvío interno del comando al líder desde un seguidor | El contrato y Java responden `NOT_LEADER` con `leaderHint` | Sin reenvío; `NOT_LEADER` + `leaderHint` | `test_follower_answers_not_leader_with_hint` |
| Idempotencia solo por `requestId` | El contrato deduplica por `(clientId, clientRequestId)` y exige `REQUEST_ID_CONFLICT` | Búsqueda por el par y comparación de bytes | `test_leader_deduplicates_and_detects_conflicts` |
| Estado corrupto se reiniciaba en blanco | Perder `votedFor` permite votar dos veces en un término | `StorageError` y el nodo no arranca | `test_corrupt_state_is_not_silently_reset` |
| Truncado del log en un conflicto de `prevLog` sin protección | Podía borrar el prefijo comprometido | Solo se trunca por encima de `commitIndex` | — |

### Pendiente de la fase II (no corregido en la fase I)

| Bug | Ubicación (`raft_engine.py`) | Riesgo |
|---|---|---|
| `_step_down` borra `votedFor` sin subir el término cuando un candidato recibe AppendEntries de su mismo término | `_step_down`, llamado desde `append_entries` | Doble voto en un término |
| Falta la entrada NO_OP al asumir el liderazgo | `_become_leader` | Entradas de términos previos no se comprometen hasta que llega un comando nuevo |
| Un hilo nuevo por peer en cada heartbeat y elección, sin límite de RPC en vuelo | `_replicate_to_all_peers`, `_start_election` | Acumulación de hilos con un peer lento o caído; respuestas fuera de orden |
| `sleep` con el lock tomado (retardo artificial de fallos) | `_available` | Bloquea todo el motor mientras dura el retardo |
| Cada escritura reescribe todo el estado (JSON completo) | `storage.py` | Coste O(n) por entrada: relevante para el benchmark de la fase VIII |
| Sin bloqueo exclusivo del directorio de datos | `storage.py` | Dos procesos con el mismo `--data-dir` se pisarían (Java sí lo bloquea) |
| `tools/benchmark_and_report.py` sigue en el protocolo JSON anterior y usa `matplotlib` (dependencia externa) | herramienta | No ejecutable hasta migrarlo (fase VIII) |
| `secondary_engine.py` (2PC) sigue importando `protocol_dm2` y tiene los bugs listados en CLAUDE.md | 2PC | Fase III |

## Clúster mixto Java + Python — fase I (sub-paso I.4)

```bash
python3 tools/mixed_cluster_test.py     # Windows: py -3 tools\mixed_cluster_test.py
```

n1 Java + n2 Python + n3 Python en `127.0.0.1:19101-19103` con `--data-dir` temporales. Dos
rondas, una con líder inicial Java y otra con líder inicial Python. El lenguaje del líder se
fuerza arrancando primero ese nodo para que llegue con un término mayor. En cada ronda:
elección; 20 `CLIENT_COMMAND` concurrentes que siguen `NOT_LEADER`; avance de `commitIndex` en
los seguidores del otro lenguaje; muerte del líder, reelección y 10 comandos más; reinicio del
nodo caído y comparación por `--inspect` de Java y de Python de todas las entradas comprometidas.
Al final se revisan los logs de los nodos.

Resultado real: 5 corridas seguidas, **PASS 14/14** en cada una. En la ronda con líder Java, los
seguidores Python aceptaron lotes de hasta 3 entradas (según el `matchIndex` registrado por Java)
y avanzaron su `commitIndex`. En la ronda con líder Python, el seguidor Java aceptó sus
AppendEntries hasta `commitIndex` 20 y, tras revivir, se puso al día (paso 5 de la ronda Java).
Los logs de los nodos no tienen Traceback, `ERROR`, `rpc-discarded` (respuestas descartadas por
el motor Python) ni `rpc-error`.

Para comparar también los bytes del comando, `--inspect` agrega `command=<hex>` en Java y Python.
Es un cambio aditivo de la CLI que no toca la lógica Raft.

### Observaciones del clúster mixto para la fase II

| Observación | Evidencia | Relación |
|---|---|---|
| El líder Python no agrega NO_OP | Ronda con líder Python: 30 entradas comprometidas, 0 NO_OP (Java agrega 1) | Bug NO_OP ya listado |
| Plazos de elección distintos: Python 350–700 ms, Java 500–900 ms | En las 10 reelecciones medidas ganó siempre un nodo Python | Igualar los plazos con Java para no sesgar las elecciones |
| Sin verificar en Windows: `os.replace` puede fallar con `PermissionError` si otro proceso (antivirus, indexador o un `--inspect` simultáneo) tiene abierto `raft_state.json`; hoy eso detiene el nodo (fail-stop) | Toda la evidencia de la fase I se obtuvo en Ubuntu | Probar en n2 Windows antes de la demo LAN |
| Los logs del motor Python se imprimen con el lock tomado (Java hace lo mismo): si nadie lee un `stdout` redirigido a un pipe, el nodo se bloquea | Revisión de cierre de la fase I | Redirigir siempre a archivo o consola (los scripts ya lo hacen) |
| Java no registra las respuestas DM2 que descarta | `TcpClient.call` lanza `IOException` sin log | La revisión de logs solo cubre los descartes del lado Python |
