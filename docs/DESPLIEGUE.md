# Despliegue en LAN

1. Compilar en cada equipo: `python3 tools/build_all.py` (Windows: `py -3 tools/build_all.py`).
2. Anotar la IP de cada equipo (`ip -4 addr` en Ubuntu, `ipconfig` en Windows).
3. Abrir solo el puerto TCP del nodo para las IP del equipo:

   ```bash
   # Ubuntu (n1)
   sudo ufw allow from 192.168.1.102 to any port 9101 proto tcp
   sudo ufw allow from 192.168.1.103 to any port 9101 proto tcp
   ```
   ```powershell
   # Windows (n2; para n3 usar el puerto 9103)
   New-NetFirewallRule -DisplayName "DogMessenger n2" -Direction Inbound -Protocol TCP `
     -LocalPort 9102 -RemoteAddress 192.168.1.101,192.168.1.103 -Action Allow
   ```
4. Arrancar los tres nodos con los comandos del [README](../README.md) y comprobar
   la conexión con `--ping`.
5. Para 2PC, agregar `--algo 2pc --coordinator n1` (`-Algo 2pc -Coordinator n1` en
   PowerShell) y usar otro `--data-dir`.
6. Copiar `config/cluster.chat.raft.example.json` (o `cluster.chat.2pc.example.json`)
   y reemplazar `127.0.0.1` por las IP reales antes de arrancar el gateway.

`DOG_CHAT_KEY` debe tener el mismo valor en los nodos y en el gateway. Las claves, los
certificados y `data/` no se suben al repositorio.
