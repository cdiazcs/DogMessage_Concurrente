# Dog Messenger · PC4 CC4P1 2026-II

Sistema de mensajería distribuida (desktop, web y móvil) cuyo historial se replica
entre tres nodos mediante **Raft** y **Two-Phase Commit (2PC)**, implementados desde
cero con sockets TCP e hilos de la biblioteca estándar de cada lenguaje.

**Universidad Nacional de Ingeniería · Programación Concurrente y Distribuida**

| Nodo | Lenguaje | Equipo | Integrante |
|---|---|---|---|
| `n1` | Java (LP1) | Ubuntu | Cristian Diaz |
| `n2` | Python (LP2) | Windows | Maurizio Berdiales |
| `n3` | Kotlin/JVM (LP3) | Windows |Enoc Rosales|

## Funcionalidades

- **Consenso:** Raft (elección, replicación, compromiso, recuperación) y 2PC con
  coordinador fijo. Los tres lenguajes forman un mismo clúster con el protocolo binario DM2.
- **Chat:** usuarios, conversaciones privadas y grupales con orden consistente,
  texto, imágenes y archivos.
- **Cifrado punto a punto:** los nodos solo almacenan y replican texto cifrado.
- **Vinculación de dispositivos:** un dispositivo nuevo recupera el historial
  antiguo desde el log replicado.
- **Clientes:** desktop (Java Swing), web (navegador) y Android, conectados mediante
  un gateway TLS.

## Requisitos

- JDK 17 o superior (`java`, `javac`)
- Python 3.10 o superior (`python3` en Ubuntu, `py -3` en Windows)
- `kotlinc` para compilar el nodo Kotlin
- Android Studio para el cliente móvil

## Compilar

```bash
python3 tools/build_all.py --desktop      # Windows: py -3 tools/build_all.py --desktop
```

## Ejecutar el clúster en tres computadoras

Reemplaza las IP de ejemplo por las reales. Para 2PC usa `--algo 2pc --coordinator n1`
(o `-Algo 2pc -Coordinator n1`) y un directorio de datos distinto.

**n1 · Ubuntu (Java)**
```bash
bash scripts/ubuntu_nodo1.sh --id n1 --host 0.0.0.0 --port 9101 --data-dir data/n1 \
  --algo raft --chat-feed --members n1,n2,n3 \
  --peer n2=192.168.1.102:9102 --peer n3=192.168.1.103:9103
```

**n2 · Windows (Python)**
```powershell
.\scripts\windows_nodo2.ps1 -Id n2 -BindHost 0.0.0.0 -Port 9102 -DataDir data/n2 `
  -Algo raft -ChatFeed -Members 'n1,n2,n3' -Peer @('n1=192.168.1.101:9101','n3=192.168.1.103:9103')
```

**n3 · Windows (Kotlin)**
```powershell
.\scripts\windows_nodo3.ps1 -Id n3 -BindHost 0.0.0.0 -Port 9103 -DataDir data/n3 `
  -Algo raft -ChatFeed -Members 'n1,n2,n3' -Peer @('n1=192.168.1.101:9101','n2=192.168.1.102:9102')
```

Verificar la conectividad entre nodos:
```bash
java -cp build/java dogmessenger.NodeMain --id n1 --ping 192.168.1.102:9102
```

Abre en el firewall solo el TCP entrante del puerto del nodo, limitado a las IP del
equipo. Detalles en [docs/DESPLIEGUE.md](docs/DESPLIEGUE.md).

## Gateway y clientes

```bash
python3 tools/gateway_test_certs.py data/gateway-tls
export DOG_CHAT_KEY="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
export DOG_GATEWAY_KEY="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
python3 gateway/chat_gateway.py --config config/cluster.chat.raft.example.json \
  --origin https://localhost:8443 --cert data/gateway-tls/server.pem --key data/gateway-tls/server.key
```

- **Desktop:** `bash scripts/desktop.sh -Djavax.net.ssl.trustStore=data/gateway-tls/client.jks`
  (Windows: `.\scripts\desktop.ps1 -TrustStore data/gateway-tls/client.jks`)
- **Web:** abrir `https://localhost:8443`
- **Android:** `cd clients/android/DogMessengerApp && ./gradlew assembleDebug` e instalar el APK

Las claves y certificados se generan para cada ensayo y no se incluyen en el repositorio.

## Demostración

1. **Nodos sincronizados:** enviar mensajes desde desktop, web y Android y comprobar
   que los tres nodos tienen el mismo historial (`--inspect`).
2. **Caída del líder:** detener el líder Raft y observar la elección de uno nuevo
   con un término mayor.
3. **Caída y recuperación:** detener un seguidor, seguir enviando mensajes, reiniciarlo
   y verificar que se pone al día.
4. **2PC:** repetir con `--algo 2pc` y mostrar el bloqueo cuando cae el coordinador.
5. **Comparación:** tiempo de consenso por mensaje en Raft y en 2PC (ver Rendimiento).

## Pruebas

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
python3 tools/trilingual_cluster_test.py      # Raft con Java, Python y Kotlin
python3 tools/two_phase_commit_test.py        # 2PC con caídas y recuperación
python3 tools/chat_cluster_test.py            # chat sobre Raft y 2PC
```
En Windows reemplaza `python3` por `py -3`.

## Rendimiento

```bash
python3 tools/benchmark_and_report.py --config config/cluster.local.json
```
Genera CSV, gráficos y la tabla comparativa en `evidencias/benchmark/RESULTADOS.md`.

| Métrica | Raft | 2PC |
|---|---|---|
| Latencia p50 / p95 | _completar_ | _completar_ |
| Throughput (msg/s) | _completar_ | _completar_ |
| Mensajes por commit | _completar_ | _completar_ |
| Tolera caída de 1 nodo | Sí (mayoría 2/3) | No (requiere a todos) |

## Estructura

```
nodes/java      Nodo n1: Raft, 2PC, chat y almacenamiento de archivos
nodes/python    Nodo n2: Raft, 2PC y chat
nodes/kotlin    Nodo n3: Raft, 2PC y chat
gateway/        Gateway TLS entre clientes y clúster
clients/        Desktop (Swing), web y Android
protocol/       Contratos de los protocolos (DM2, 2PC, chat, cifrado, multimedia)
tools/          Compilación, pruebas, demostración y benchmark
docs/           Arquitectura y despliegue
evidencias/     Resultados de pruebas y rendimiento
```
