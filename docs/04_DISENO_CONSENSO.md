# Diseño pendiente de consenso — guía de implementación

**NADA DE ESTE DOCUMENTO ESTÁ IMPLEMENTADO EN EL STARTER.** Se incluye para evitar que el equipo tome decisiones incompatibles.

## Contrato interno por lenguaje (misma semántica, diferente código)

```text
ConsensusEngine:
    start()
    stop()
    propose(command, requestId) -> resultado pendiente o comprometido
    committedEntries() -> entradas confirmadas en orden
    leader() -> id o desconocido
    metrics() -> contadores/timing
```

## Raft, tareas de cada nodo

- Persistir `currentTerm`, `votedFor` y entradas del log antes de responder de forma que comprometa la seguridad.
- Estados follower/candidate/leader, elección con timeout aleatorio, `RequestVote` (chequeo de último índice/term).
- `AppendEntries` con `prevLogIndex`, `prevLogTerm`, `entries`, `leaderCommit`.
- Rechazar entradas incompatibles y retroceder para reparar registros divergentes; guardar `nextIndex`/`matchIndex` del líder.
- Aplicar solo entradas comprometidas; regla de compromiso por mayoría y restricción de período del líder actual.
- No responder al cliente como definitivamente entregado antes de que se confirme.
- Sincronizar acceso al estado compartido desde hilos de red, elección y persistencia.
- Reintentos de mensajes de cliente idempotentes (`requestId`).
- Recuperar entradas faltantes tras caída y antes de afirmar sincronización completa.

## Segundo algoritmo

Debe ser el algoritmo **que asigne el profesor**, no uno elegido arbitrariamente. En grupos de 3 se pide Raft + un segundo. Si asignan PBFT: con tres réplicas no se tolera una falla bizantina (para f=1 se requiere n>=3f+1=4). Si asignan 2PC/3PC: comparar sus diferentes garantías, sin equipararlos a consenso de log replicado tolerante a fallos.

## Cómo comparar honestamente

Reutilizar el mismo cliente, tamaño y cantidad de mensajes, red, hardware y metodología. Medir tiempo hasta compromiso por reloj monotónico del mismo proceso, throughput (confirmados/segundos), mensajes de protocolo, elección y recuperación. Repetir pruebas y registrar errores. Nunca rellenar resultados antes de ejecutar.

## Especificación que el grupo debe aprobar antes de codificar

La propuesta está en el [contrato DM2](../protocol/CONTRATO_TCP_v2.md). Incluye
el formato binario, el registro de tipos, los diagramas y los fixtures. Revisar
el contrato entre los tres integrantes antes de implementar los codecs y las RPC
según el [plan de interoperabilidad](../protocol/PLAN_INTEROPERABILIDAD_DM2.md).
