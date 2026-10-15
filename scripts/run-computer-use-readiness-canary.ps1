param(
    [Parameter(Mandatory = $true)][string]$HermesExe,
    [Parameter(Mandatory = $true)][string]$HermesHome
)

$ErrorActionPreference = "Stop"
$stateDir = Join-Path $HermesHome "state"
$failurePath = Join-Path $stateDir "computer_use_readiness_canary_launcher_failure.json"

try {
    if ([IO.Path]::GetExtension($HermesExe) -ieq ".cmd") {
        & $env:ComSpec /d /s /c ('"{0}" --run-module hermes_cli.computer_use_readiness_canary --hermes-home "{1}"' -f $HermesExe, $HermesHome)
    } else {
        & $HermesExe --run-module hermes_cli.computer_use_readiness_canary --hermes-home $HermesHome
    }
    $exitCode = $LASTEXITCODE
} catch {
    $exitCode = 127
}

if ($exitCode -eq 0) {
    Remove-Item -LiteralPath $failurePath -Force -ErrorAction SilentlyContinue
    exit 0
}

New-Item -ItemType Directory -Force -Path $stateDir | Out-Null
$payload = @{
    schema_version = 1
    host = $env:COMPUTERNAME
    checked_at = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    ok = $false
    phase = "public-launcher"
    error_class = "LauncherExit"
    exit_code = $exitCode
}
$temporary = "$failurePath.$PID.tmp"
$payload | ConvertTo-Json -Compress | Set-Content -LiteralPath $temporary -Encoding UTF8
Move-Item -LiteralPath $temporary -Destination $failurePath -Force

try {
    eventcreate /T ERROR /ID 100 /L APPLICATION /SO HermesComputerUse `
        /D ("Hermes computer-use readiness mismatch; host={0}; profile=all; phase=public-launcher; error=LauncherExit; exit={1}" -f $env:COMPUTERNAME, $exitCode) `
        *> $null
} catch {}
exit $exitCode
