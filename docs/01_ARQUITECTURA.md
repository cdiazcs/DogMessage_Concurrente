# Arquitectura de referencia — Dog Messenger PC4

```text
        LAN / WiFi (3 equipos distintos)

    N1 Ubuntu           N2 Windows            N3 Windows
    Java                Python                Kotlin/JVM
    TCP 9101            TCP 9102              TCP 9103
          \                |                /
           \______________ | ______________/
                 TCP con contrato común
              [Raft + algoritmo 2 futuros]
                          |
                   Chats y grupos
                          |
        cliente desktop / cliente Android / web si procede
```

## Distinción fundamental

- **Nodo de consenso**: servidor con log replicado y participación en elecciones/confirmación. Deben existir al menos 3, en máquinas diferentes.
- **Cliente**: UI que se conecta a un nodo. No es una réplica Raft. El cliente Android NO reemplaza el nodo Kotlin.
- **Red privada**: los servidores escuchan en TCP y se conectan por las IP reales de la LAN. No exponer públicamente.

## Estado implementado v0.1.0

- Servidores TCP en Java, Python, Kotlin.
- Framing de longitud prefijada compatible en tres lenguajes.
- Hilo por conexión en Python; pools de hilos en Java/Kotlin.
- Respuesta PONG con correlación de requestId.

## Arquitectura objetivo pendiente

1. **Consenso**: 3 implementaciones equivalentes de Raft y del algoritmo 2 asignado; log persistente y sincronización segura.
2. **Mensajería**: chats privados y grupos; orden derivado de entradas comprometidas.
3. **Seguridad**: cifrado de extremo a extremo real entre clientes; los servidores replican cargas cifradas. No implementar criptografía casera.
4. **Adjuntos**: almacenamiento y recuperación replicados de contenido cifrado, hash y transferencia por bloques.
5. **Clonación QR**: token temporal y vínculo autorizado; transferencia de claves de dispositivo con mecanismo seguro; recuperación de historial antiguo.
6. **Observabilidad**: archivos CSV con latencia, throughput, tiempo de recuperación y cantidad de mensajes de protocolo.
7. **Interfaz**: Android en Kotlin, desktop; web requiere confirmación del alcance por su incompatibilidad con TCP arbitrario en navegador.

## Invariantes para pruebas

- Lo confirmado por mayoría en Raft no debe desaparecer por caída de un nodo.
- Una minoría aislada no confirma nuevos mensajes.
- Réplicas recuperadas deben ponerse al día antes de afirmar igualdad de historiales.
- `PING/PONG` no prueba ninguna de estas propiedades.
