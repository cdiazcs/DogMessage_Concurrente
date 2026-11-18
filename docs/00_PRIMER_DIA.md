# Primer día en equipo — lista comprobable

- [ ] Una persona crea repositorio GitHub vacío `dog-messenger-pc4`, inicializa el ZIP como `main` y hace push.
- [ ] Los otros dos clonan esa URL; los 3 están en el mismo commit (`git rev-parse HEAD`).
- [ ] Todos leen y aprueban `protocol/CONTRATO_TCP_v1.md`.
- [ ] Instalan JDK, Python y compilador Kotlin en la máquina que corresponde; verifican `--version`.
- [ ] Cada uno ejecuta las pruebas unitarias de Python y compila su lenguaje.
- [ ] Una máquina con las 3 herramientas ejecuta `python tools/smoke_test.py`: 6 pruebas cruzadas OK.
- [ ] Configuran IP y firewall LAN, levantan 3 nodos reales, hacen PING bidireccional.
- [ ] Actualizan `docs/03_RUBRICA_TRAZABILIDAD.md` con pruebas de red reales y capturas.
- [ ] Abren 3 Issues: estado persistente de Raft en Java, Python y Kotlin.
- [ ] Cada uno crea su rama **desde main**, un PR y solicita revisión.

**Cierre esperado:** interoperabilidad TCP en 3 lenguajes y 3 máquinas. Aún no es consenso, clonación ni chat.
