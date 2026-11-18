$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
New-Item -ItemType Directory -Force -Path 'build/kotlin' | Out-Null
kotlinc nodes/kotlin/src/NodeMain.kt -include-runtime -d build/kotlin/dog-messenger-kotlin.jar
if ($LASTEXITCODE -ne 0) { throw 'Falló compilación Kotlin' }
java -jar build/kotlin/dog-messenger-kotlin.jar --id n3 --host 0.0.0.0 --port 9103
