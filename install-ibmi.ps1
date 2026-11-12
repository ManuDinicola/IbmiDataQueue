# Installe les programmes COBOL de la demo (ibmi/*.cbl) sur l'IBM i : cree le membre source, le compile avec CRTBNDCBL
# et declare la procedure SQL qui l'enveloppe (appelable en .NET avec CALL). Passe par le conteneur Docker (pilote ODBC IBM i).
# Prerequis : Docker Desktop demarre, variable utilisateur IBMI_PASSWORD definie.
# Les sources sont du COBOL au format fixe (code colonne 8). SENDDQ.cbl vise la data queue IDINICOLA1/DEMOQ : adapter DQ-LIB / DQ-NAME.
param(
    [string]$Server = 'pub400.com',
    [string]$User = 'IDINICOLA',
    [string]$Library = 'IDINICOLA1'
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
$password = [Environment]::GetEnvironmentVariable('IBMI_PASSWORD', 'User')
if (-not $password) { throw "IBMI_PASSWORD n'est pas defini (variable utilisateur)." }

# Programme -> declaration SQL de la procedure (langage COBOL, pas CBLLE).
$programs = [ordered]@{
    HELLOCBL = 'IN P_NAME CHAR(20), OUT P_GREET CHAR(40), OUT P_RC INTEGER'
    SENDDQ   = 'IN P_ID CHAR(6), IN P_TYPE CHAR(4), IN P_PAYLOAD CHAR(20)'
}

$sql = New-Object System.Collections.Generic.List[string]
foreach ($name in $programs.Keys) {
    $file = Join-Path 'ibmi' "$name.cbl"
    $sql.Add("CALL QSYS2.QCMDEXC('RMVM FILE($Library/QCBLLESRC) MBR($name)')")   # erreur ignoree si le membre n'existe pas
    $sql.Add("CALL QSYS2.QCMDEXC('ADDPFM FILE($Library/QCBLLESRC) MBR($name) SRCTYPE(CBLLE)')")
    $sql.Add("CREATE OR REPLACE ALIAS QTEMP.SRC_$name FOR $Library.QCBLLESRC ($name)")
    $seq = 0
    foreach ($line in Get-Content $file) {
        $seq++
        $text = $line.Replace("'", "''")
        $sql.Add("INSERT INTO QTEMP.SRC_$name (SRCSEQ, SRCDAT, SRCDTA) VALUES($seq, 0, '$text')")
    }
    $sql.Add("CALL QSYS2.QCMDEXC('CRTBNDCBL PGM($Library/$name) SRCFILE($Library/QCBLLESRC) SRCMBR($name)')")
    $sql.Add("CREATE OR REPLACE PROCEDURE $Library.$name ($($programs[$name])) LANGUAGE COBOL PARAMETER STYLE GENERAL EXTERNAL NAME $Library.$name")
}
$sql.Add("SELECT ROUTINE_NAME, EXTERNAL_LANGUAGE FROM QSYS2.SYSROUTINES WHERE ROUTINE_SCHEMA='$Library'")

$tmp = Join-Path ([IO.Path]::GetTempPath()) 'dq-install-ibmi'
New-Item -ItemType Directory -Force $tmp | Out-Null
Set-Content -Path (Join-Path $tmp 'install.sql') -Value $sql -Encoding ascii
$env:IBMI_PASSWORD = $password
docker build -q -t dq-odbc .devcontainer | Out-Null
$env:IBMI_SERVER = $Server
$env:IBMI_USER = $User
$output = docker run --rm -e IBMI_PASSWORD -e IBMI_SERVER -e IBMI_USER -v "${tmp}:/sql:ro" dq-odbc bash -c 'isql -b -k "Driver={IBM i Access ODBC Driver};System=$IBMI_SERVER;Uid=$IBMI_USER;Pwd=$IBMI_PASSWORD;" < /sql/install.sql' 2>&1
Remove-Item -Recurse -Force $tmp
$output | Where-Object { $_ -notmatch 'SQLRowCount returns [01]$' }
if ($output -match 'SQLDriverConnect') { throw "Connexion impossible a $Server (utilisateur, mot de passe ou reseau)." }
Write-Host "Termine. Le premier [ISQL]ERROR est normal (RMVM sur un membre absent) ; la liste ci-dessus doit montrer HELLOCBL et SENDDQ."
