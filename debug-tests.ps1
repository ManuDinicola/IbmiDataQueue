# Lance les tests d'integration IBM i dans un conteneur Linux (pilote ODBC IBM i) et attend Visual Studio :
#   Debug > Attach to Process > Connection type "Docker (Linux Container)" > dqtest > testhost
# Les sources sont copiees dans le conteneur (rien n'est ecrit dans votre dossier) et PathMap ramene les
# symboles vers vos fichiers Windows, donc les points d'arret de Visual Studio fonctionnent.
# Prerequis : Docker Desktop demarre, variable utilisateur IBMI_PASSWORD definie.
param([string]$Filter = 'Category=Integration', [switch]$NoDebug)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
$env:IBMI_PASSWORD = [Environment]::GetEnvironmentVariable('IBMI_PASSWORD', 'User')
if (-not $env:IBMI_PASSWORD) { throw "IBMI_PASSWORD n'est pas defini (variable utilisateur)." }
$env:IBMI_HOST = 'pub400.com'; $env:IBMI_USER = 'IDINICOLA'; $env:IBMI_LIBRARY = 'IDINICOLA1'
$env:IBMI_ODBC_DRIVER = 'IBM i Access ODBC Driver'
if ($NoDebug) { Remove-Item Env:VSTEST_HOST_DEBUG -ErrorAction SilentlyContinue } else { $env:VSTEST_HOST_DEBUG = '1' }
docker build -q -t dq-odbc .devcontainer | Out-Null
docker rm -f dqtest 2>$null | Out-Null
$win = "$PSScriptRoot".TrimEnd('\')
$cmd = "mkdir /work && cd /src && tar --exclude=bin --exclude=obj --exclude=.git -cf - . | tar -xf - -C /work && cd /work && " +
       "dotnet test tests/IbmiDataQueue.IntegrationTests -p:DqTfm=net10.0 '-p:PathMap=/work=$win' --filter '$Filter'"
docker run --rm --name dqtest -e IBMI_PASSWORD -e IBMI_HOST -e IBMI_USER -e IBMI_LIBRARY -e IBMI_ODBC_DRIVER -e IBMI_KEEP_QUEUE -e VSTEST_HOST_DEBUG `
  -v "${PWD}:/src:ro" dq-odbc bash -c $cmd
