# Fase III · Elección Raft en Java

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
comando del proceso detenido con el mismo directorio para recuperar su término
y voto y observar cómo sigue al líder vigente. Omitir `--data-dir` mantiene el
modo de transporte de fase II. `--ping` conserva la operación DM1 en ambos modos.

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
- `term-state.bin` guarda término, voto, identidad y membresía. Un lock impide
  usar simultáneamente el mismo directorio. La actualización sincroniza el
  archivo temporal, lo renombra de forma atómica y sincroniza el directorio.
  El modo Raft requiere un sistema de archivos local que admita estas
  operaciones. Si falla una escritura, el nodo deja de participar en Raft.
- La última posición del log es el origen `(0,0)`. El voto compara primero
  término del log y luego índice. Se aceptan heartbeats vacíos con origen y
  se rechazan lotes de entradas. La creación de NO_OP, replicación y
  compromiso quedan para la fase de log. Los comandos reciben `NOT_LEADER`
  o `RETRY` según el rol.
- Una réplica aislada no puede ganar una elección. Un líder ya elegido puede
  conservar el rol mientras está aislado hasta observar un término mayor.
  Esta fase carece de comprobación periódica de quórum.

## Pruebas reproducibles

```bash
out=$(mktemp -d)
javac --release 17 -Xlint:all -Werror -d "$out" \
  nodes/java/src/dogmessenger/NodeMain.java nodes/java/test/dogmessenger/*.java
java -cp "$out" dogmessenger.RaftElectionTest
java -cp "$out" dogmessenger.TransportTest
python3 -m unittest discover -s tests -p 'test_*.py' -v
python3 tools/smoke_test.py
```

La suite de elección abre procesos Java y directorios temporales independientes.
El ensayo de caída mata al líder de forma abrupta y verifica la reelección,
la recuperación del proceso y que los logs observados registren como máximo
un líder por término. Las pruebas de transporte usan los fixtures DM2 aprobados.
