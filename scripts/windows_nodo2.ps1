# .\scripts\windows_nodo2.ps1 -N1 192.168.1.101:9101 -N3 192.168.1.103:9103
[CmdletBinding(PositionalBinding = $false)]
param(
    [string]$N1,
    [string]$N3,
    [ValidatePattern('\A[a-zA-Z0-9_.-]+\z')]
    [string]$BindHost = '0.0.0.0',
    [ValidateRange(1, 65535)]
    [int]$Port = 9102,
    [ValidateScript({ -not [string]::IsNullOrWhiteSpace($_) })]
    [string]$DataDir = 'data/n2',
    [ValidatePattern('\A[a-zA-Z0-9_-]{1,32}\z')]
    [string]$Id = 'n2',
    [ValidateSet('raft', '2pc')]
    [string]$Algo = 'raft',
    [string]$Coordinator,
    [string[]]$Peer = @(),
    [string]$Members,
    [switch]$Diagnostic,
    [switch]$ChatFeed,
    [switch]$Inspect
)

$ErrorActionPreference = 'Stop'
if ($ChatFeed -and ($Diagnostic -or $Inspect -or $BindHost -ne '127.0.0.1')) {
    throw '-ChatFeed requiere servidor de consenso en 127.0.0.1.'
}
if ($Algo -eq '2pc' -and (-not $Coordinator -or $Diagnostic -or $DataDir -eq 'data/n2')) {
    throw '2PC requiere -Coordinator y un -DataDir separado.'
}
if ($Coordinator -and $Algo -ne '2pc') { throw '-Coordinator requiere -Algo 2pc.' }
if ($Diagnostic -and $Inspect) { throw 'Selecciona -Diagnostic o -Inspect.' }
if ($N1) { $Peer += "n1=$N1" }
if ($N3) { $Peer += "n3=$N3" }
$nodeArgs = @('--id', $Id, '--host', $BindHost, '--port', $Port)
if ($ChatFeed) { $nodeArgs += '--chat-feed' }
if ($Diagnostic) {
    if ($Peer.Count -or $Members) { throw '-Diagnostic no admite peers.' }
} else {
    if (-not $Peer.Count) { throw 'Configura los peers Raft con -Peer o -N1/-N3.' }
    $nodeArgs += @('--algo', $Algo, "--data-dir=$DataDir")
    if ($Coordinator) { $nodeArgs += @('--coordinator', $Coordinator) }
    foreach ($endpoint in $Peer) {
        if ($endpoint -notmatch '\A[a-zA-Z0-9_-]{1,32}=[a-zA-Z0-9_.-]+:([0-9]{1,5})\z' -or
            [int]$Matches[1] -lt 1 -or [int]$Matches[1] -gt 65535) {
            throw "Peer invalido '$endpoint'. Usa ID=HOST:PUERTO."
        }
        $nodeArgs += @('--peer', $endpoint)
    }
    if ($Members) { $nodeArgs += @('--members', $Members) }
    if ($Inspect) { $nodeArgs += '--inspect' }
}

Push-Location (Join-Path $PSScriptRoot '..')
try {
    py -3 nodes/python/node.py @nodeArgs
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Pop-Location
}
