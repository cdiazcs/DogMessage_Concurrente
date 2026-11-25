# Nodo n2 (Python, Raft sobre DM2 binario). Uso:
#   .\scripts\windows_nodo2.ps1 -N1 192.168.1.101:9101 -N3 192.168.1.103:9103
# Las IP por defecto son de EJEMPLO: reemplazarlas por las reales de la LAN.
param(
    [string]$N1 = '192.168.1.101:9101',
    [string]$N3 = '192.168.1.103:9103',
    [string]$DataDir = 'data\n2',
    [int]$Port = 9102
)
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
py -3 nodes\python\node.py --id n2 --host 0.0.0.0 --port $Port --data-dir $DataDir `
    --peer "n1=$N1" --peer "n3=$N3"
if ($LASTEXITCODE -ne 0) { throw "El nodo n2 termino con codigo $LASTEXITCODE" }
