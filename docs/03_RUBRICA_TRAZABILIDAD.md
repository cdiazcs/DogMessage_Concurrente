# Lista de requisitos y evidencia — PC4 2026-II

**Fuente:** `P4cc4P1-262_v04.pdf`, pp. 1-5. El enunciado **no indica puntajes por criterio**. No atribuir ponderaciones inventadas. Estado inicial: TODO salvo conectividad.

| Requisito | Estado v0.1 | Prueba/evidencia que debe existir al entregar | PR |
|---|---|---|---|
| 3 nodos en computadoras distintas LAN/WiFi | PARCIAL: ejecutables; LAN por comprobar | Video 3 equipos + puertos/IP + pruebas de comunicación | — |
| LP1, LP2, LP3, solo SDK y sockets/hilos | PARCIAL: Java, Python, Kotlin TCP | Código comentado y build de tres nodos | — |
| Raft desde cero | PARCIAL: elección, replicación, compromiso y recuperación en Java y loopback | Integración entre lenguajes y pruebas LAN | — |
| Segundo algoritmo asignado desde cero | PENDIENTE | Implementación en 3 nodos + comparación con Raft | — |
| Comunicación cliente desktop/móvil | PENDIENTE | Mensajes en ambos sentidos | — |
| Versión web (objetivo general) | POR DEFINIR | Confirmar alcance con docente | — |
| Chat privado/grupal y orden consistente | PENDIENTE | 3 usuarios y orden compartido | — |
| Imágenes y archivos | PENDIENTE | Hash de integridad + recuperación de contenido | — |
| Clonación mediante QR | PENDIENTE | Sincronización de historial anterior y autorización | — |
| Cifrado de extremo a extremo | PENDIENTE | Prueba cliente-cliente y servidor sin texto legible | — |
| Fallo del líder / nueva elección | PARCIAL: prueba automática con tres procesos Java en loopback | Logs con término/líder y demo en vivo | — |
| Caída de seguidor / recuperación | PENDIENTE | Igualdad de índices/entradas comprometidas | — |
| Partición de red | PENDIENTE | Mayoría/minoría y explicación | — |
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
