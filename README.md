# Dog Messenger · PC4 2026-II — BASE `main` (v0.1.0)

> **Estado: starter funcional de conectividad, NO es el proyecto terminado.**
> Los tres servidores en Java, Python y Kotlin comparten un protocolo TCP sin bibliotecas externas de comunicación.
> **Implementado:** Raft con persistencia en Java y en Python, interoperables en DM2 binario ([contrato](protocol/CONTRATO_TCP_v2.md)) y probados en un clúster mixto local.
> **No están implementados aún:** Raft en Kotlin, el segundo algoritmo asignado, chat, QR, móvil, web, transferencia de archivos, cifrado E2EE, comparaciones de rendimiento.

Este repositorio está preparado para que **3 integrantes** trabajen en ramas cortas e independientes, incorporen cambios por Pull Request y validen lo que exige el enunciado de PC4.

## 0. ¿Qué contiene el ZIP?

| Parte | Estado inicial | Responsable sugerido |
|---|---|---|
| `nodes/java` | Servidor/cliente TCP PING/PONG compilable | Ubuntu (integrante 1) |
| `nodes/python` | Servidor/cliente TCP PING/PONG ejecutable | Windows (integrante 2) |
| `nodes/kotlin` | Servidor/cliente TCP PING/PONG compilable | Windows (integrante 3) |
| `protocol/` | Contrato DM1, versión y ejemplos | Los 3 revisan |
| `tests/`, `tools/` | Pruebas unitarias y prueba cruzada local | Los 3 |
| `clients/` | Carpetas y notas de implementación futura | Interfaces |
| `docs/` | Arquitectura, criterios PC4 y plan de ramas | Los 3 |
| `.github/` | Plantilla de PR y verificaciones iniciales | Los 3 |

## 1. Requisitos del equipo

- **Java JDK 17 o superior**: comandos `java` y `javac`.
- **Python 3.10 o superior**: `python`/`py -3` en Windows; `python3` en Ubuntu.
- **Kotlin/JVM compiler**: comando `kotlinc` (compatible con JDK 17 o superior). Este compilador se instala como herramienta de desarrollo. No se distribuyen bibliotecas de comunicación.
- **Git** y cuenta en GitHub, con acceso al mismo repositorio.
- Las 3 computadoras en una **red LAN/WiFi que permita conexiones entre equipos**; abrir TCP **9101, 9102 y 9103** en el firewall local si es necesario.

> Kotlin/JVM (servidor) y Kotlin Android (cliente futuro) son componentes distintos. El nodo Kotlin incluido NO es una aplicación Android.

## 2. Probar todo en UNA computadora antes de conectar la LAN

Desde la carpeta raíz del repositorio:

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v
python3 tools/smoke_test.py
```

En Windows, cambia `python3` por `py -3` si corresponde:

```powershell
py -3 -m unittest discover -s tests -p 'test_*.py' -v
py -3 tools/smoke_test.py
```

La prueba compila Java/Kotlin, inicia tres procesos locales en puertos **19101-19103** y verifica **6 llamadas cruzadas** entre los tres lenguajes. `PASS` confirma únicamente conectividad y compatibilidad del contrato inicial, no consenso ni replicación.

## 2b. Nodo Python con Raft (DM2 binario)

`nodes/python/node.py` acepta el mismo CLI que `NodeMain` de Java: `--id`, `--host`, `--port`,
`--data-dir`, `--peer ID=HOST:PUERTO` (uno por cada otra réplica), `--members`, `--ping` e
`--inspect`, más `--algo raft` (2PC llega en la fase III). Sin `--data-dir` el nodo solo atiende
PING DM1 y cierra las RPC Raft, igual que Java.

Clúster local de tres nodos Python (una terminal por nodo):

```bash
# Ubuntu
python3 nodes/python/node.py --id n1 --host 127.0.0.1 --port 9101 --data-dir data/n1 --peer n2=127.0.0.1:9102 --peer n3=127.0.0.1:9103
python3 nodes/python/node.py --id n2 --host 127.0.0.1 --port 9102 --data-dir data/n2 --peer n1=127.0.0.1:9101 --peer n3=127.0.0.1:9103
python3 nodes/python/node.py --id n3 --host 127.0.0.1 --port 9103 --data-dir data/n3 --peer n1=127.0.0.1:9101 --peer n2=127.0.0.1:9102
```

```powershell
# Windows
py -3 nodes\python\node.py --id n1 --host 127.0.0.1 --port 9101 --data-dir data\n1 --peer n2=127.0.0.1:9102 --peer n3=127.0.0.1:9103
py -3 nodes\python\node.py --id n2 --host 127.0.0.1 --port 9102 --data-dir data\n2 --peer n1=127.0.0.1:9101 --peer n3=127.0.0.1:9103
py -3 nodes\python\node.py --id n3 --host 127.0.0.1 --port 9103 --data-dir data\n3 --peer n1=127.0.0.1:9101 --peer n2=127.0.0.1:9102
```

Estado persistido de un nodo (mismos argumentos más `--inspect`; muestra cada entrada con su
estado `COMMITTED`/`PENDING` y los bytes del comando en hex). En Java, detener antes el nodo,
porque bloquea su directorio:

```bash
python3 nodes/python/node.py --id n2 --data-dir data/n2 --peer n1=127.0.0.1:9101 --peer n3=127.0.0.1:9103 --inspect
```

```powershell
py -3 nodes\python\node.py --id n2 --data-dir data\n2 --peer n1=127.0.0.1:9101 --peer n3=127.0.0.1:9103 --inspect
```

Partición simulada de un nodo Python (para demos): crear `<data-dir>/fault.json` con
`{"isolated": true}`; el nodo deja de responder, de enviar y de procesar respuestas. Con
`{"isolated": false}` (o borrando el archivo) se reincorpora.

Pruebas de consenso (los datos y logs quedan en un directorio temporal que el script imprime):

```bash
python3 tools/fault_simulator.py       # 3 nodos Python: partición, caída y recuperación
python3 tools/mixed_cluster_test.py    # n1 Java + n2/n3 Python, con líder Java y con líder Python
```

```powershell
py -3 tools\fault_simulator.py
py -3 tools\mixed_cluster_test.py
```

## 3. Arrancar en las tres computadoras reales

Todos deben clonar la misma versión de `main` (o extraer el ZIP para pruebas). **Ejemplos de IP, no direcciones reales del equipo:**

| Equipo | IP de EJEMPLO | Nodo | Puerto |
|---|---|---|---|
| Ubuntu | `192.168.1.101` | `n1` Java | `9101` |
| Windows A | `192.168.1.102` | `n2` Python | `9102` |
| Windows B | `192.168.1.103` | `n3` Kotlin/JVM | `9103` |

### Ubuntu: nodo 1

```bash
bash scripts/ubuntu_nodo1.sh
```

### Windows A: nodo 2

```powershell
.\scripts\windows_nodo2.ps1 -N1 192.168.1.101:9101 -N3 192.168.1.103:9103
```

El script ejecuta el nodo Python con Raft:
`py -3 nodes\python\node.py --id n2 --host 0.0.0.0 --port 9102 --data-dir data\n2 --peer n1=192.168.1.101:9101 --peer n3=192.168.1.103:9103`.
El equivalente en Ubuntu es
`python3 nodes/python/node.py --id n2 --host 0.0.0.0 --port 9102 --data-dir data/n2 --peer n1=192.168.1.101:9101 --peer n3=192.168.1.103:9103`.
Para que Java participe en Raft, `n1` debe arrancarse con `--data-dir` y los dos `--peer`
(ver [docs/06_RAFT_JAVA.md](docs/06_RAFT_JAVA.md)). El nodo Kotlin aún no implementa
la replicación DM2 (fase IV).

### Windows B: nodo 3

```powershell
.\scripts\windows_nodo3.ps1
```

### Verificar desde CUALQUIER equipo

En Ubuntu, hacia Windows A:

```bash
java -cp build/java dogmessenger.NodeMain --id n1 --ping 192.168.1.102:9102
```

En Windows A, hacia Ubuntu:

```powershell
py -3 nodes/python/node.py --id n2 --ping 192.168.1.101:9101
```

En Windows B, hacia Ubuntu (después de ejecutar el script, que compila el JAR):

```powershell
java -jar build/kotlin/dog-messenger-kotlin.jar --id n3 --ping 192.168.1.101:9101
```

Cambien las IP de EJEMPLO por las IP reales. `--host 0.0.0.0` escucha interfaces locales, **NO** significa que deban exponer los puertos a Internet. Use una red privada de confianza.

### Puertos y firewall

- En Windows, validar conectividad con `Test-NetConnection 192.168.1.101 -Port 9101` desde PowerShell.
- En Ubuntu, revisar `ss -lnt | grep 9101`.
- Autorizar entradas TCP en el firewall **solo para LAN privada y puertos necesarios**.
- Si hay aislamiento entre clientes WiFi, conectarse a una red que permita tráfico entre dispositivos.

## 4. Convertir este ZIP en la rama `main` de GitHub

Desde la carpeta extraída (solo UNA persona hace el primer push):

```bash
git init
git branch -M main
git add .
git commit -m "chore: base inicial PC4 con nodos TCP y protocolo DM1"
git remote add origin https://github.com/ORGANIZACION/dog-messenger-pc4.git
git push -u origin main
```

**Sustituir la URL de ejemplo por la URL verdadera** del repositorio vacío creado en GitHub. Si Git solicita nombre/correo: configurar `git config --global user.name ...` y `git config --global user.email ...`.

Los otros integrantes **NO ejecutan `git init` para compartir código**: clonan la URL real:

```bash
git clone https://github.com/ORGANIZACION/dog-messenger-pc4.git
cd dog-messenger-pc4
```

Una vez que exista `main`, configurar su protección: PR obligatorio, una revisión de un compañero y pruebas antes del merge. Revisar `CONTRIBUTING.md`.

## 5. Primera rama de cada integrante

```bash
git switch main
git pull --ff-only origin main
git switch -c feat/java-raft-skeleton
```

Sustituye el nombre según el trabajo. **No crear ramas desde otras ramas de compañeros.** Las interfaces se acuerdan en `protocol/` antes de que los componentes dependan de su implementación. Usar tests locales y simuladores (*mocks*) cuando otra parte no esté lista.

## 6. Documentos que deben leer todos

1. [`CONTRIBUTING.md`](CONTRIBUTING.md): convención de Git y PR.
2. [`protocol/CONTRATO_TCP_v1.md`](protocol/CONTRATO_TCP_v1.md): contrato exactamente implementado en los 3 lenguajes.
3. [`docs/01_ARQUITECTURA.md`](docs/01_ARQUITECTURA.md): topología y límites técnicos.
4. [`docs/02_PLAN_RAMAS.md`](docs/02_PLAN_RAMAS.md): plan de trabajo independiente y apoyo cruzado.
5. [`docs/03_RUBRICA_TRAZABILIDAD.md`](docs/03_RUBRICA_TRAZABILIDAD.md): evidencia requerida y pendientes.
6. [`docs/04_DISENO_CONSENSO.md`](docs/04_DISENO_CONSENSO.md): tareas correctas de Raft y algoritmo 2.
7. [`docs/05_DECISIONES_ABIERTAS.md`](docs/05_DECISIONES_ABIERTAS.md): puntos a acordar con el docente.

## 7. Compromiso de honestidad técnica

No afirmar que un `PONG` comprueba consenso. El starter demuestra conectividad TCP, *framing* interoperable y uso inicial de hilos. **Para pasar la PC4 deben completar e integrar realmente el sistema y documentar pruebas.**

**Fuente de requisitos:** `P4cc4P1-262_v04.pdf`, páginas 1-5 (documento de clase aportado por el grupo). El ZIP no contiene los PDF originales, evitando aumentar su tamaño y distribuir materiales innecesarios.
