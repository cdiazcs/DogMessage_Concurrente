# Hitos y ramas independientes

## Fase I y ramas por lenguaje

| Rama | Responsabilidad | Archivos principales | Criterio de integración |
|---|---|---|---|
| `main` | Base estable compartida | Repositorio integrado | PR revisado y pruebas de regresión |
| `java` | Nodo Java en Ubuntu | `nodes/java/` | Codec y RPC Java con los fixtures comunes |
| `python` | Nodo Python en Windows | `nodes/python/` | Codec y RPC Python con los fixtures comunes |
| `kotlin` | Nodo Kotlin/JVM en Windows | `nodes/kotlin/` | Codec y RPC Kotlin con los fixtures comunes |
| `docs/dm2` | Diseño de fase I | `protocol/`, `docs/` | Revisión del contrato y fixtures por los tres integrantes |

Integrar primero el [contrato DM2](../protocol/CONTRATO_TCP_v2.md) mediante un PR de
`docs/dm2` hacia `main`. Tras su aprobación, sincronizar `java`, `python` y `kotlin`
desde `main`. Usar ramas cortas derivadas de `main` para tareas acotadas según
[CONTRIBUTING](../CONTRIBUTING.md) y dirigir los PR a `main`. Evitar dependencias
entre ramas de lenguajes usando los fixtures compartidos y pares simulados hasta
que las tres implementaciones estén disponibles. Las RPC de consenso se implementan
en las fases siguientes.

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
