$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
py -3 nodes/python/node.py --id n2 --host 0.0.0.0 --port 9102
