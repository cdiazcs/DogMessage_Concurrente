# CONTRIBUTING — Cómo colaborar sin bloquearse

## Política de ramas

- `main` es estable; no se trabaja directamente en ella.
- Cada rama sale de la última versión de `main`. Nombre: `feat/area-tarea`, `fix/area-tarea`, `test/area-caso` o `docs/tema`.
- Una rama = una funcionalidad acotada. Pull Requests pequeños, revisables y con pruebas.
- Nunca hacer que una rama A dependa de código existente solamente en la rama B.
- Para desacoplar, definir el contrato primero, usar *mocks* en pruebas y mantener componentes compilables.
- Si un cambio requiere alterar `protocol/`, abrir un PR separado y hacer que los tres lo revisen antes de implementar los consumidores.

## Flujo por tarea

```bash
git switch main
git pull --ff-only origin main
git switch -c feat/python-log-storage
# Editar archivos de tu módulo, escribir pruebas, ejecutar pruebas
git add nodes/python tests docs
git commit -m "feat(python): persistir registro local"
git push -u origin feat/python-log-storage
```

Abrir PR dirigido a `main`; un integrante distinto revisa. Tras aprobar e integrar:

```bash
git switch main
git pull --ff-only origin main
git branch -d feat/python-log-storage
```

Si `main` cambió antes de integrar tu rama, sincronízala con `main` y vuelve a ejecutar pruebas (puedes usar merge para evitar reescribir ramas compartidas). No hacer `push --force` sobre `main`.

## Reglas de revisión

El PR debe responder:
- ¿Qué requisito PC4 cubre?
- ¿Qué contrato utiliza/cambia?
- ¿Qué pruebas se ejecutaron realmente, con resultado?
- ¿Qué está incompleto o necesita revisión?
- ¿Cómo revertir si rompe el clúster?

## Coordinación para tres integrantes

- **I1 Ubuntu/Java:** arquitectura TCP, implementación Java de Raft y algoritmo asignado; apoyo a correcciones de integración.
- **I2 Windows/Python:** implementación Python de ambos algoritmos, recuperación, métricas, pruebas automatizadas.
- **I3 Windows/Kotlin:** implementación Kotlin de ambos algoritmos, Android y clonación cuando corresponda.
- Los tres deben comprender las reglas de Raft, el protocolo y el despliegue: ningún nodo puede ser un simple cliente que no participe en consenso.
- Cualquiera puede crear una rama para ayudar a otro **si anuncia el Issue correspondiente** y modifica un conjunto acotado de archivos; la persona responsable revisa el PR.

## Definition of Done (DoD)

- [ ] Compila o ejecuta sin bibliotecas de comunicación/consenso de terceros.
- [ ] Se respetó `protocol/` o se aprobó primero su cambio.
- [ ] Hay prueba reproducible de éxito y de error.
- [ ] No hay contraseñas, claves ni IP privadas personales guardadas en Git.
- [ ] Se actualizó `docs/03_RUBRICA_TRAZABILIDAD.md` cuando aplica.
- [ ] PR revisado por alguien diferente al autor.

## Convención de commits

`feat(java): ...`, `feat(kotlin): ...`, `feat(python): ...`, `fix(protocol): ...`, `test(cluster): ...`, `docs: ...`.

## Cuidado con los binarios

No subir `build/`, `.class`, `.jar`, carpetas del IDE ni reportes con datos confidenciales. Para la entrega final, comprimir solo las fuentes, informes PDF y presentaciones PDF que requiera el docente. Este ZIP es la **base de desarrollo**, no la entrega final.
