# Decisiones pendientes con profesor / equipo

1. **Segundo algoritmo asignado:** escribir el nombre exacto. Tres integrantes ⇒ Raft y segundo algoritmo; solo se solicita un tercero a grupos de cuatro.
2. **Versión web:** aparece en objetivo general, pero los requisitos detallados priorizan desktop y móvil; el navegador no abre sockets TCP arbitrarios. Consultar si aceptan HTTP implementado sobre sockets sin frameworks.
3. **QR:** validar si se puede utilizar un mecanismo nativo permitido o si exigen implementar codificador/lector QR desde cero; no asumir que una biblioteca externa está autorizada.
4. **Cifrado E2EE:** validar si las bibliotecas criptográficas incluidas en JDK/Android/SDK son suficientes y el mecanismo de vinculación de claves que aprueba el docente. Jamás crear algoritmos criptográficos propios.
5. **Algoritmos de fallo bizantino:** si se asigna PBFT y se exige demostrar tolerancia a f=1, hacen falta al menos cuatro réplicas; tres PCs podrían alojar un cuarto proceso pero deberán documentarlo y pedir aprobación si se requieren PCs diferentes.
6. **Lenguaje LP1:** si fue específicamente asignado por el docente, ajustar el reparto para respetarlo.
7. **Fechas, tiempos y notas:** el enunciado recibido no incluye un puntaje detallado; no inventar rúbrica de 20 puntos.
