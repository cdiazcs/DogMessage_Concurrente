# Hitos y ramas independientes

## Fase inicial (crear PR hacia main)

| Área | Rama inicial | Archivos principales | Prueba de aceptación |
|---|---|---|---|
| Java, Ubuntu | `feat/java-raft-state` | `nodes/java/` | Tests de término/voto y arranque |
| Python, Windows | `feat/python-raft-state` | `nodes/python/` | Tests de término/voto y arranque |
| Kotlin, Windows | `feat/kotlin-raft-state` | `nodes/kotlin/` | Tests de término/voto y arranque |
| Contrato conjunto | `docs/raft-wire-v2` | `protocol/` | Aprobado por los tres **antes** de consumidores |

Cada integrante puede desarrollar estados locales mientras se aprueba el formato de mensajes Raft. No enviar aún RPC entre nodos hasta que el contrato esté estable.

## Fases siguientes

- Elección (`RequestVote`) independiente en cada lenguaje con un fixture común.
- Heartbeat y replicación (`AppendEntries`) independiente con fixture común.
- Log, persistencia y recuperación con ensayos de caída.
- Segundo algoritmo asignado, con interfaz intercambiable.
- Chat desktop y Android, con servidor simulado hasta integrar el clúster.
- Grupos, archivos, cifrado, clonación QR y reportes.

## Cómo ayudar a otro integrante sin crear dependencia

1. La persona que necesita apoyo abre o asigna un Issue, describiendo el fallo y cómo reproducirlo.
2. El ayudante actualiza su `main` y crea **una nueva rama desde `main`**, ej. `fix/python-duplicate-entry`.
3. Solo cambia lo necesario y adjunta prueba.
4. El dueño del módulo revisa y aprueba; después el ayudante cierra su rama.

**No hacer:** una rama `integrante2` de varias semanas, una rama nacida de la rama de otra persona ni copiar código manualmente entre carpetas de Git.

## Automatización y responsabilidad compartida

Cada PR debe probar su lenguaje y el contrato. En `main`, antes de una demo, ejecutar `tools/smoke_test.py` y pruebas de clúster real (cuando existan). El CI inicial compila Java, prueba Python y, cuando el runner tenga compilador, verifica Kotlin; no equivale a probar 3 máquinas reales.
