# Reporte Automatizado de Rendimiento — Dog Messenger (PC4)

## Comparativa de Algoritmos de Consenso

| Algoritmo | Confirmados | Errores | Throughput (msg/s) | Latencia Media (ms) | p50 (ms) | p95 (ms) | p99 (ms) | RPCs de Red |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **RAFT** | 100/100 | 0 | 76.52 | 13.049 | 15.096 | 26.025 | 30.199 | 240 |
| **2PC** | 100/100 | 0 | 27.75 | 36.021 | 36.719 | 60.49 | 74.028 | 400 |

## Observaciones Técnicas
- **Raft** compromete entradas al alcanzar mayoría (2 de 3 nodos), tolerando la caída de 1 nodo sin detener el servicio.
- **2PC (Two-Phase Commit)** requiere unanimidad de participantes en la fase `PREPARE`, bloqueando el compromiso si un nodo cae durante la transacción.