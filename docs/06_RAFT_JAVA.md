# Raft en Java · Elección, replicación y compromiso

## Ejecutar tres procesos locales

Desde la raíz del repositorio, compilar con JDK 17 o superior y preparar
un directorio para cada réplica.

```bash
out=build/java
state=build/raft-java
mkdir -p "$out" "$state"/{n1,n2,n3}
javac --release 17 -d "$out" nodes/java/src/dogmessenger/NodeMain.java
```

Ejecutar un comando por terminal desde la raíz. En cada terminal usar
`out=build/java` y `state=build/raft-java`.

```bash
java -cp "$out" dogmessenger.NodeMain --id n1 --host 127.0.0.1 --port 9101 \
  --data-dir "$state/n1" --peer n2=127.0.0.1:9102 --peer n3=127.0.0.1:9103

java -cp "$out" dogmessenger.NodeMain --id n2 --host 127.0.0.1 --port 9102 \
  --data-dir "$state/n2" --peer n1=127.0.0.1:9101 --peer n3=127.0.0.1:9103

java -cp "$out" dogmessenger.NodeMain --id n3 --host 127.0.0.1 --port 9103 \
  --data-dir "$state/n3" --peer n1=127.0.0.1:9101 --peer n2=127.0.0.1:9102
```

Buscar `role=LEADER` en las salidas y detener ese proceso. Los dos procesos
restantes pueden elegir un líder con término mayor. Volver a ejecutar el
comando del proceso detenido con el mismo directorio para recuperar su término,
voto y log local y observar cómo sigue al líder vigente. Omitir `--data-dir` mantiene el
modo de transporte de fase II. `--ping` conserva la operación DM1 en ambos modos.

Para probar un comando y la recuperación de un seguidor, detener un proceso
seguidor y abrir otra terminal. Sustituir el puerto y el ID de ejemplo por los
del líder observado.

```bash
jshell --class-path build/java <<'EOF'
import dogmessenger.NodeMain.*;
import java.net.InetSocketAddress;
import java.util.UUID;
var endpoint = new InetSocketAddress("127.0.0.1", 9101);
var request = new Message(new Header(1, MessageCodec.CLIENT_COMMAND, UUID.randomUUID(), "c1"), new ClientCommand(new byte[] {0, 42}));
new TcpClient().call(endpoint, "n1", request).payload();
/exit
EOF
```

Con dos nodos disponibles, esperar `COMMITTED` (`status=0`) y un `logIndex`
positivo. Ante una espera agotada, `RETRY` (`status=2`) deja el resultado
indeterminado. Reenviar la misma variable `request` conserva el ID y los
bytes. Con el líder aislado, un comando nuevo queda pendiente. Reiniciar
el seguidor con el mismo comando de arranque y directorio. En los logs del
líder, `event=replicated` muestra el `matchIndex` y `event=applied` muestra
el compromiso aplicado.

## Diseño y alcance

Las reglas de elección y de actualización de término siguen el
[contrato DM2](../protocol/CONTRATO_TCP_v2.md) y la figura 2 del
[artículo de Raft](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-extended.pdf).

- La membresía es fija y se deduce del ID local más los `--peer`. Si también se
  indica `--members`, debe contener exactamente esos mismos IDs. En el clúster
  de tres, la mayoría es dos votos. Al comenzar una elección, el candidato
  persiste primero el término y su voto propio.
- Cada réplica sortea un plazo electoral de 500 a 900 ms y el líder envía
  heartbeats cada 125 ms. Un único monitor serializa los cambios de estado.
  Las llamadas TCP salientes se ejecutan fuera de ese monitor y cada peer
  tiene como máximo un intento activo.
- `term-state.bin` guarda término, voto, identidad, membresía y `commitIndex`.
  Los archivos de fases anteriores se leen con un índice inicial de cero.
  Un lock impide
  usar simultáneamente el mismo directorio. La actualización sincroniza el
  archivo temporal, lo renombra de forma atómica y sincroniza el directorio.
  El modo Raft requiere un sistema de archivos local que admita estas
  operaciones. Si falla una escritura, el nodo deja de participar en Raft.
- El voto usa la última posición recuperada del log y compara primero el
  término y luego el índice. Un nuevo líder guarda un `NO_OP` local antes de
  anunciarse. Los heartbeats vacíos comprueban el prefijo alcanzado por
  cada seguidor.
- Una réplica aislada no puede ganar una elección. Un líder ya elegido puede
  conservar el rol mientras está aislado hasta observar un término mayor.
  Esta fase carece de comprobación periódica de quórum.

## Log local de fase IV-A

`raft-log.bin` se crea vacío al abrir un directorio de fase III. Cada registro
incluye una firma `DLG1`, longitud de cuerpo, complemento de la longitud,
índice explícito, contenido de la entrada según DM2 y un CRC32C sobre el cuerpo
desde el índice. El archivo es local a Java. El protocolo TCP conserva el
índice implícito de cada entrada.

`RaftLog` serializa escritores y lectores, asigna índices consecutivos y
publica cada entrada después de que `FileChannel.force(true)` termine.
`lastPosition()` devuelve índice y término en una sola consulta.
`raft-log.lock` excluye otros escritores entre procesos y el almacenamiento
recupera todas las entradas en memoria. Si al arrancar encuentra un último
registro incompleto, conserva el prefijo verificado y sincroniza el truncamiento.
Una cabecera completa inválida, un CRC incorrecto o una secuencia inválida
impiden abrir el log. Un append con error de escritura requiere reabrir el
almacenamiento.

## Replicación de fase IV-B

El líder construye un lote a partir de `nextIndex` de cada seguidor y limita
el cuerpo DM2 a 1 048 576 bytes, incluidos encabezado y campos fijos, y el lote
a 256 entradas. Un rechazo retrocede el índice de búsqueda. Una desconexión
conserva los índices y deja el reintento para el siguiente ciclo. La
conexión a cada seguidor progresa de manera independiente.

El receptor exige que coincidan índice y término previos. Conserva las
entradas coincidentes, trunca desde el primer conflicto y sincroniza los
reemplazos antes de confirmar el lote. Un corte durante la reparación puede
dejar un prefijo más corto, que el líder reenvía tras el reinicio.

## Compromiso y recuperación de fase IV-C

- El líder cuenta su propia escritura durable y el `matchIndex` confirmado
  de cada seguidor. Avanza `commitIndex` solo si una mayoría tiene una
  entrada del término vigente. El `NO_OP` de la elección permite que el
  mismo avance comprometa el prefijo de términos anteriores.
- El seguidor limita `leaderCommit` a la última posición validada por la
  RPC exitosa y conserva el máximo con su índice anterior. Una reparación
  que tocaría el prefijo comprometido detiene Raft en ese nodo.
- El índice comprometido se escribe después del log y antes de aplicar o
  contestar al cliente. `lastApplied` avanza de uno en uno. La máquina de
  estados de esta fase es un registro de comandos por par
  `(clientId, requestId)` que conserva el índice de la primera aplicación.
  Un `NO_OP` avanza el índice aplicado sin añadir un comando. Al reiniciar,
  el nodo reconstruye ese registro en memoria recorriendo el prefijo
  comprometido. Los efectos externos a ese registro quedan fuera de esta fase.
- El líder devuelve el mismo índice al reintentar un comando aplicado y
  `REQUEST_ID_CONFLICT` si el mismo par conserva bytes distintos. Para un
  comando pendiente espera hasta 750 ms con un máximo de ocho esperas
  simultáneas. Una espera agotada o sin cupo devuelve `RETRY`. La pérdida
  de mayoría impide confirmar una entrada nueva, aunque se haya escrito
  localmente.
- `consensusNanos` mide desde antes de escribir localmente la entrada hasta
  persistir el compromiso, usando el reloj monotónico del líder. Los logs
  muestran `-` cuando el nodo carece de una muestra local. Los tiempos se
  mantienen en memoria y la reconstrucción no estima tiempos históricos.

Los eventos de rol muestran `commitIndex`, `lastApplied` y `lastLogIndex`.
Cada evento de aplicación incluye índice, término, tipo y si produjo un
efecto nuevo. Para inspeccionar todo el log, detener el proceso y añadir
`--inspect` a su comando de arranque. El comando muestra el estado
recuperado como seguidor y clasifica cada entrada en `COMMITTED` o
`PENDING`. La API local `RaftNode.snapshot()` expone la misma información
y el log completo en memoria.

## Pruebas reproducibles

```bash
out=$(mktemp -d)
javac --release 17 -Xlint:all -Werror -d "$out" \
  nodes/java/src/dogmessenger/NodeMain.java nodes/java/test/dogmessenger/*.java
java -cp "$out" dogmessenger.RaftLogTest
java -cp "$out" dogmessenger.RaftElectionTest
java -cp "$out" dogmessenger.TransportTest
python3 -m unittest discover -s tests -p 'test_*.py' -v
python3 tools/smoke_test.py
```

La suite de elección abre procesos Java y directorios temporales independientes.
El ensayo de caída mata al líder de forma abrupta y verifica la reelección,
la recuperación del proceso y que los logs observados registren como máximo
un líder por término. El ensayo de replicación comprueba un lote de más de
1 MiB dividido en mensajes, un seguidor atrasado, un sufijo divergente y el
reenvío tras desconexión. También comprueba la confirmación con 2 de 3,
la pérdida de mayoría con una entrada pendiente, la reelección después
de una confirmación y la reconciliación del antiguo líder. La prueba del
término vigente retiene el `NO_OP` para verificar que replicar solo
entradas antiguas en mayoría no las compromete. Las pruebas de transporte
usan los fixtures DM2 aprobados.
