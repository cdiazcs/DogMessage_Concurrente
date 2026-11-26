# Nodo n3 (Kotlin/JVM), almacenamiento Raft y diagnostico TCP DM1.
[CmdletBinding(PositionalBinding = $false)]
param(
    [ValidatePattern('\A[a-zA-Z0-9_.-]+\z')]
    [string]$BindHost = '0.0.0.0',
    [ValidateRange(1, 65535)]
    [int]$Port = 9103,
    [ValidateScript({ -not [string]::IsNullOrWhiteSpace($_) })]
    [string]$DataDir = 'data/n3',
    [ValidatePattern('\A[a-zA-Z0-9_-]{1,32}\z')]
    [string]$Id = 'n3',
    [switch]$Diagnostic,
    [switch]$ChatFeed,
    [switch]$Inspect,
    [switch]$ExperimentalRaft,
    [ValidateSet('raft', '2pc')]
    [string]$Algo,
    [string]$Coordinator,
    [string[]]$Peer = @(),
    [string]$Members
)

$ErrorActionPreference = 'Stop'
if ($ChatFeed -and ($Diagnostic -or $Inspect -or $BindHost -ne '127.0.0.1')) {
    throw '-ChatFeed requiere servidor de consenso en 127.0.0.1.'
}
if ($Algo -eq '2pc' -and (-not $Coordinator -or $Diagnostic -or $DataDir -eq 'data/n3')) {
    throw '2PC requiere -Coordinator y un -DataDir separado.'
}
if ($Coordinator -and $Algo -ne '2pc') { throw '-Coordinator requiere -Algo 2pc.' }
if ($Diagnostic -and ($Inspect -or $Algo -or $ExperimentalRaft -or $Peer.Count -or $Members)) {
    throw '-Diagnostic requiere un arranque sin peers.'
}
if (($Inspect -or $Algo) -and $ExperimentalRaft) { throw 'Selecciona -Inspect o -ExperimentalRaft.' }
if (($ExperimentalRaft -or ($Algo -and -not $Inspect)) -and $Peer.Count -eq 0) { throw 'Raft requiere -Peer.' }
if (($Peer.Count -gt 0 -or $Members) -and -not ($ExperimentalRaft -or $Algo -or $Inspect)) {
    throw 'Los peers requieren -Algo raft, -ExperimentalRaft o -Inspect.'
}
Push-Location (Join-Path $PSScriptRoot '..')
try {
    py -3 tools/build_all.py --kotlin-only
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    $nodeArgs = @('--id', $Id, '--host', $BindHost, '--port', $Port)
    if ($ChatFeed) { $nodeArgs += '--chat-feed' }
    if ($Diagnostic) { $nodeArgs += '--diagnostic' } else { $nodeArgs += @('--data-dir', $DataDir) }
    if ($Inspect) { $nodeArgs += '--inspect' }
    if ($ExperimentalRaft) { $nodeArgs += '--experimental-raft' }
    if ($Algo) { $nodeArgs += @('--algo', $Algo) }
    if ($Coordinator) { $nodeArgs += @('--coordinator', $Coordinator) }
    foreach ($endpoint in $Peer) { $nodeArgs += @('--peer', $endpoint) }
    if ($Members) { $nodeArgs += @('--members', $Members) }
    java -jar build/kotlin/dog-messenger-kotlin.jar @nodeArgs
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Pop-Location
}
