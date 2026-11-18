# DM2 · Secuencias Raft

Cada intento abre su propia conexión TCP. La réplica devuelve el mismo `requestId`.
Las secuencias describen el comportamiento previsto por el [contrato](CONTRATO_TCP_v2.md).

## RequestVote

```mermaid
sequenceDiagram
    participant C as Candidato
    participant R as Réplica
    Note over C: Incrementa y persiste término y voto propio
    C->>R: 0x01 · term, lastLog
    alt Trama o emisor inválidos
        Note over R: Cierra o envía ERROR según contrato
    else RPC válida
        opt Término recibido superior
            Note over R: Pasa a seguidor y persiste término y voto libre
        end
        alt Término vigente, voto disponible y log al día
            Note over R: Persiste voto por el candidato
            R-->>C: 0x81 · currentTerm, true
        else Término obsoleto, log atrasado o voto ocupado
            R-->>C: 0x81 · currentTerm, false
        end
    end
    Note over C: Correlaciona respuesta y verifica réplica
    Note over C: Término superior implica pasar a seguidor
    Note over C: Mayoría de la elección vigente habilita liderazgo
```

## AppendEntries

```mermaid
sequenceDiagram
    participant L as Líder
    participant F as Seguidor
    L->>F: 0x02 · term, prevLog, leaderCommit, entries
    Note over F: Valida RPC y actualiza término si es superior
    alt Término obsoleto o posición previa incompatible
        F-->>L: 0x82 · currentTerm, false, 0
        Note over L: Ante término superior pasa a seguidor
        Note over L: Si sigue líder y hay conflicto, reduce nextIndex y reintenta
    else Posición previa coincide
        Note over F: Conserva coincidencias o repara y persiste el sufijo
        Note over F: Limita compromiso al último índice confirmado por la RPC
        F-->>L: 0x82 · currentTerm, true, prevLog.index + entryCount
        Note over L: Verifica correlación e índice y actualiza progreso del seguidor
    end
    Note over L,F: Un heartbeat lleva cero entradas y también comprueba prevLog
```
