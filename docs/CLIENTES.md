# Clientes

Los tres clientes se conectan al gateway: TLS en el puerto 9443 (desktop y Android)
o HTTPS en el 8443 (web).

## Certificados de ensayo

```bash
python3 tools/gateway_test_certs.py data/gateway-tls --san 192.168.1.101
keytool -importcert -noprompt -alias dog-ca -file data/gateway-tls/ca.pem \
  -keystore data/gateway-tls/client.jks -storepass changeit
```

Esto genera `ca.pem`, `server.pem` y `server.key`, válidos por 14 días. Por defecto
incluyen `localhost`, `127.0.0.1` y `10.0.2.2` (emulador Android). Usa `--san` para
agregar la IP del gateway.

## Desktop (Java Swing)

```bash
bash scripts/desktop.sh --build -Djavax.net.ssl.trustStore=data/gateway-tls/client.jks
```
En Windows: `.\scripts\desktop.ps1 -TrustStore data/gateway-tls/client.jks`. Indica
el host y el puerto (9443) en la pantalla de inicio.

## Web

Abre `https://localhost:8443` (o el valor de `--origin` del gateway) y confía en
`ca.pem` en el navegador.

## Android

```bash
cd clients/android/DogMessengerApp && ./gradlew assembleDebug
adb install app/build/outputs/apk/debug/app-debug.apk
```
Requiere JDK 21 y Android SDK 36.1. Instala `ca.pem` como certificado de usuario
(la compilación debug lo acepta). En el emulador, el gateway del equipo es
`10.0.2.2:9443`.

## Vincular un dispositivo

En el dispositivo nuevo, inicia sesión y elige «Generar QR». Desde un dispositivo ya
vinculado, autoriza ese descriptor antes de que caduque (120 s por defecto,
`--link-ttl`). Al aprobarlo, el dispositivo nuevo recibe el historial anterior.
