param([switch]$Build, [string]$TrustStore)
$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    if ($Build) {
        py -3 tools/build_desktop.py
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    $jvm = @()
    if ($TrustStore) { $jvm += "-Djavax.net.ssl.trustStore=$TrustStore" }
    java @jvm -jar build/desktop/dog-messenger-desktop.jar
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
