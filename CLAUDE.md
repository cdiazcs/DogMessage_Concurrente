# Dog Messenger · PC4 CC4P1 2026-II — Contexto para Claude Code

## Rol
Eres un experto en programación concurrente y distribuida. Ayudas a un grupo de 3 alumnos
a completar la PC4: chat distribuido (desktop, móvil, web) con log replicado por consenso.

## Requisitos del enunciado (P4cc4P1-262_v04.pdf)
- Raft obligatorio + segundo algoritmo asignado: Two-Phase Commit (2PC). Ambos desde cero.
- Lenguajes: LP1 Java (n1, Ubuntu), LP2 Python (n2, Windows), LP3 Kotlin/JVM (n3, Windows).
  Cada lenguaje es un nodo distinto del MISMO clúster.
- Solo sockets TCP/UDP del SDK + hilos. PROHIBIDO: WebSockets, Socket.io, MQ, frameworks de
  comunicación, librerías de consenso. Solo librería estándar de cada lenguaje.
- Chat: usuarios, grupos con orden consistente, texto, imágenes, archivos.
- Clonación de chats vía QR = sincronizar historial antiguo desde el log replicado.
- Cifrado punto a punto: el servidor NUNCA descifra; el consenso replica solo ciphertext.
- Mínimo 3 nodos en LAN/WiFi reales.
- Demo: nodos sincronizados, caída del líder y reelección, caída y recuperación de nodo,
  comparación de tiempo de consenso Raft vs 2PC.
- Informe: arquitectura con roles, diagramas de protocolo, tabla comparativa (latencia,
  tolerancia a caídas, complejidad, nº de mensajes, recuperación, particiones), explicación
  de cómo el consenso resuelve la clonación y el historial.

## Estado actual del repo
- Java (nodes/java/src/dogmessenger/NodeMain.java): Raft completo y probado (elección,
  replicación, commit, persistencia). Es la IMPLEMENTACIÓN DE REFERENCIA. No romperla.
  CLI: --id, --host, --port, --data-dir, --peer ID=HOST:PUERTO (repetible), --members, --ping.
- Protocolo oficial: DM2 BINARIO (protocol/CONTRATO_TCP_v2.md, protocol/message-types.md,
  32 casos en protocol/fixtures/dm2.json). Todo nodo debe hablarlo. DM1 (PING/PONG) se
  conserva solo para diagnóstico.
- Kotlin (nodes/kotlin/src): solo elige líder; log en memoria, sin replicación. Además
  descifra mensajes en el servidor (desencriptarAES): eso debe eliminarse.
- Python en main: solo PING DM1.
- Rama de un compañero (../DogMessage_Concurrente-python): Python con Raft + 2PC pero en
  DM2 JSON (incompatible) y con bugs conocidos:
  * Raft: commit_index=len(log) al reiniciar; matchIndex=len(log) en vez de
    prevLogIndex+len(entries); _step_down borra votedFor sin subir término (doble voto);
    falta NO_OP al ser líder; hilos ilimitados por peer en heartbeat; sleep con lock tomado.
  * 2PC: cada nodo es coordinador; índice calculado fuera del lock (índices duplicados y
    orden distinto entre nodos, verificado con 3 clientes concurrentes); PREPARE no
    persistido; fases en serie (sesga la comparación).
  * Esa rama trae Java/Kotlin VIEJOS (v0.1): NUNCA copiarlos sobre main.
- Android (clients/android): base con QR (ZXing) y AES-CBC con IV fijo (a reemplazar).

## Plan de fases
I    Python habla DM2 binario e integra su Raft en clúster mixto con Java.
II   Corregir bugs de Raft Python (con tests).
III  2PC correcto (algorithm=0x02) en Python y Java.
IV   Kotlin completo + matriz trilingüe Raft/2PC.
V    Capa de chat sobre el log (usuarios, grupos, archivos, historial/clonación).
VI   E2EE real (ECDH + AES-GCM), QR.
VII  Clientes desktop (Swing), Android, web (HTTP a mano sobre ServerSocket).
VIII Benchmark y reportes automáticos.
IX   LAN real, guion de demo, informe y presentación.

## Reglas de trabajo
1. Trabaja SOLO en la fase/sub-paso que te indique. Antes de codificar, lee los archivos
   relevantes y dame un plan breve (archivos a tocar + pruebas). Espera mi OK si es grande.
2. Cada cambio de consenso va con su test. Ejecuta los tests y muéstrame la salida real.
   No digas "funciona" sin ejecutarlo.
3. No inventes resultados, métricas ni rúbricas. Todos los números salen de scripts.
4. Referencia de comportamiento: RaftNode de Java y figura 2 del paper de Raft.
5. Concurrencia: un lock por motor de consenso; nunca I/O de red ni sleep con el lock
   tomado; máximo un RPC en vuelo por peer; persistir (fsync + rename atómico) antes de
   responder.
6. No agregues dependencias externas. Si algo parece necesitar una, avísame primero.
7. Commits pequeños en español con prefijos feat:/fix:/test:/docs:. Una rama por fase,
   creada desde main. Al terminar, actualiza docs/03_RUBRICA_TRAZABILIDAD.md con la
   evidencia real.
8. No subas data*/, build/, __pycache__/ ni archivos temporales.
9. Al final de cada fase entrega: resumen de cambios, cómo probarlo (comandos exactos para
   Ubuntu y Windows) y qué queda pendiente.

## Multiplataforma (el equipo usa Ubuntu y Windows)
- n1 Java corre en Ubuntu; n2 Python y n3 Kotlin corren en Windows.
- Todo script/test debe funcionar en ambos: usar pathlib/Path, nunca rutas fijas.
- Comandos: `python3` en Ubuntu, `py -3` en Windows. Documenta ambos.
- Scripts de arranque: .sh para Ubuntu y .ps1 para Windows; mantenerlos equivalentes.
- No hacer fsync de directorios en código que corra en Windows.
- No reformatear ni cambiar finales de línea de archivos que no forman parte de la tarea.
- No modificar código de otro integrante fuera de la fase pedida sin avisar.