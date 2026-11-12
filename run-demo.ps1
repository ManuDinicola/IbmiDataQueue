# Lance l'application web de demo (samples/IbmiDataQueue.Demo.Web) dans un conteneur Linux (pilote ODBC IBM i)
# puis ouvrez http://localhost:5080 . Ctrl+C pour arreter.
# Pour deboguer : Visual Studio > Debug > Attach to Process > Docker (Linux Container) > dqdemo > processus "dotnet" (IbmiDataQueue.Demo.Web.dll).
# Les sources sont copiees dans le conteneur (rien n'est ecrit dans votre dossier) et PathMap ramene les
# symboles vers vos fichiers Windows, donc les points d'arret de Visual Studio fonctionnent.
# Prerequis : Docker Desktop demarre, variable utilisateur IBMI_PASSWORD definie.
param([int]$Port = 5080)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
$env:IBMI_PASSWORD = [Environment]::GetEnvironmentVariable('IBMI_PASSWORD', 'User')
if (-not $env:IBMI_PASSWORD) { throw "IBMI_PASSWORD n'est pas defini (variable utilisateur)." }
$env:IBMI_HOST = 'pub400.com'; $env:IBMI_USER = 'IDINICOLA'; $env:IBMI_LIBRARY = 'IDINICOLA1'
$env:IBMI_ODBC_DRIVER = 'IBM i Access ODBC Driver'
docker build -q -t dq-odbc .devcontainer | Out-Null
docker rm -f dqdemo 2>$null | Out-Null
$win = "$PSScriptRoot".TrimEnd('\')
$cmd = "mkdir /work && cd /src && tar --exclude=bin --exclude=obj --exclude=.git -cf - . | tar -xf - -C /work && cd /work && " +
       "dotnet build samples/IbmiDataQueue.Demo.Web -p:DqTfm=net10.0 '-p:PathMap=/work=$win' -v q --nologo && " +
       "cd samples/IbmiDataQueue.Demo.Web && ASPNETCORE_URLS=http://0.0.0.0:$Port dotnet bin/Debug/net10.0/IbmiDataQueue.Demo.Web.dll"
Write-Host "Application : http://localhost:$Port"
docker run --rm --name dqdemo -p "${Port}:${Port}" -e IBMI_PASSWORD -e IBMI_HOST -e IBMI_USER -e IBMI_LIBRARY -e IBMI_ODBC_DRIVER -e IBMI_QUEUE `
  -v "${PWD}:/src:ro" dq-odbc bash -c $cmd
