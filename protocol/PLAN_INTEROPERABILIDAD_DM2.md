# Plan de pruebas de interoperabilidad DM2

Fase I aporta vectores y resultados esperados. La ejecución DM2 queda pendiente de
los codecs y nodos de las siguientes fases. Usar solo SDK, sockets TCP y pares
simulados cuando falte una implementación.

## Preparación

- Probar en Java 17+, Python 3.10+ y Kotlin/JVM con JDK 17+. Registrar versiones,
  sistema operativo, revisión Git, comando, puertos y resultado observado.
- Usar los mismos [fixtures JSON](fixtures/dm2.json). Cada valor `hex` contiene la
  trama TCP completa, incluido el prefijo. Convertir pares hex a bytes antes de
  enviar. `direction` indica el lado que recibe. `expected` es el oráculo. Un
  vector con `endOfStream` requiere cerrar la escritura tras enviar sus bytes.
- Para vectores de servidor, configurar `n1`, `n2` y `n3` como réplicas y usar
  `n2` como receptor. Cada caso parte de estado aislado. Los vectores válidos
  de entrada prueban decodificación; el resultado Raft depende del estado.
- Para vectores de cliente, usar un par simulado y configurar la solicitud
  pendiente con el ID, algoritmo, tipo correspondiente y réplica esperada del
  vector válido. El vector de ID distinto conserva el ID original en la solicitud.

## Orden de ejecución y aceptación

| Prueba | Ejecución | Aceptación |
|---|---|---|
| Codec local | En los tres lenguajes leer vectores, verificar campos y volver a codificar los válidos | Igualdad byte a byte, incluyendo octetos no UTF-8, índices, términos y correlación |
| Límites | Generar cuerpo de 1 048 576 bytes y su variante +1; probar comandos de 1, 65 536 y 65 537 bytes, lotes de 0, 256 y 257 entradas, IDs de 1 y 32 bytes y R64 de 2⁶³−1 | Admitir extremos legales; rechazar exceso y suma de índices desbordada antes de aplicar estado |
| Matriz TCP | Java→Python, Java→Kotlin, Python→Java, Python→Kotlin, Kotlin→Java y Kotlin→Python. En cada dirección enviar voto, heartbeat, lote y comando mediante nodos o pares simulados | Validar payload y correlación en el receptor, y una única respuesta admisible en el iniciador |
| Flujo TCP | Fragmentar prefijo y cuerpo, incluso byte a byte; repetir en una escritura; enviar dos tramas juntas, EOF a mitad de trama y lectura que exceda el plazo | Una sola operación por conexión; cierre en tramas incompletas y sin cambio de estado |
| Errores cruzados | Enviar los inválidos a cada receptor y simular respuestas inválidas para cada iniciador. Añadir casos de largo interno incorrecto, emisor inesperado, tipo de respuesta incorrecto y algoritmo distinto | Cierre, ERROR con código e ID esperados o descarte local según el contrato; término, voto y log intactos |
| Migración | Ejecutar los PING DM1 existentes contra listeners mixtos y probar un destino solo DM1 con un cliente DM2 | PONG DM1 correlacionado; el intento DM2 falla si recibe DM1 o EOF, sin traducir RPC |
| Estado Raft | Con tres implementaciones, rotar líder. Probar voto denegado, término obsoleto, posición previa divergente, reparación y heartbeat con `leaderCommit` mayor que el lote | Comparar términos, votos, índices y entradas; persistir antes de responder y aplicar solo índices confirmados |
| Reintentos de cliente | Descartar una respuesta, repetir con el mismo ID y bytes, repetir con contenido distinto y cambiar de líder | Índice estable al confirmar, aplicación única y conflicto para bytes distintos mientras el par siga reservado |

Primero ejecutar en loopback, luego repetir la matriz y las pruebas de estado
en los tres equipos LAN. Guardar por caso bytes enviados y recibidos, captura
de salida y estado inicial y final cuando aplique. Registrar cada resultado
observado como aprobado, fallido o bloqueado. La aceptación DM2 exige los tres
codecs y las seis direcciones. La evidencia de consenso requiere los ensayos de
estado con réplicas reales.

## Regresión de Fase I

En `docs/dm2` se ejecutaron las cuatro pruebas unitarias existentes con acceso a
sockets locales. El smoke test compiló Java y Kotlin y completó los seis PING
cruzados en loopback. Esta evidencia cubre DM1.
