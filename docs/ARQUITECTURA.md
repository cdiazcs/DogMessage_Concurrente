# Arquitectura

```text
 Desktop (Swing)   Web (navegador)   Android
        \               |               /
         \     DOG/1 sobre TLS/HTTPS   /
          '-------- gateway/ ---------'
                       |  DM2 (TCP)
     n1 Java  <---->  n2 Python  <---->  n3 Kotlin
     TCP 9101         TCP 9102           TCP 9103
```

- **Nodos:** son réplicas de un mismo log. Cada nodo implementa Raft y 2PC con sockets
  TCP e hilos de la biblioteca estándar y se comunica con los demás mediante el
  [contrato DM2](../protocol/CONTRATO_TCP_v2.md). El estado se guarda en `data/<id>`.
- **Gateway:** autentica a los usuarios, convierte las operaciones DOG/1 en comandos
  del log y lee el feed de chat de los nodos. No participa en el consenso.
- **Clientes:** son interfaces de usuario. Cifran los mensajes y adjuntos antes de
  enviarlos, por lo que los nodos solo replican texto cifrado.
- **Adjuntos:** se transfieren por bloques al almacén DMB1 y en el log solo se registra
  su hash.
- **Vinculación:** un dispositivo nuevo se aprueba con un código QR temporal y recibe
  acceso al historial anterior desde el log.

## Garantías

| | Raft | 2PC |
|---|---|---|
| Confirmación | Mayoría (2 de 3) | Todos los nodos |
| Caída de un nodo | Sigue operando | Se bloquea hasta la recuperación |
| Caída del líder o coordinador | Se elige un nuevo líder | Los participantes preparados esperan |
