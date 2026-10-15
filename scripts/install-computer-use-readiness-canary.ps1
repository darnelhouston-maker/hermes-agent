param(
    [Parameter(Mandatory = $true)][string]$HermesExe,
    [Parameter(Mandatory = $true)][string]$RunnerPath,
    [Parameter(Mandatory = $true)][string]$HermesHome
)

$ErrorActionPreference = "Stop"
$taskName = "HermesComputerUseReadinessCanary"
$stableDir = Join-Path $HermesHome "scripts"
$stableRunner = Join-Path $stableDir "computer-use-readiness-canary-task.ps1"
New-Item -ItemType Directory -Force -Path $stableDir | Out-Null
$temporary = "$stableRunner.$PID.tmp"
Copy-Item -LiteralPath $RunnerPath -Destination $temporary -Force
Move-Item -LiteralPath $temporary -Destination $stableRunner -Force

$powerShell = (Get-Process -Id $PID).Path
$arguments = ('-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}" -HermesExe "{1}" -HermesHome "{2}"' -f $stableRunner, $HermesExe, $HermesHome)
$action = New-ScheduledTaskAction -Execute $powerShell -Argument $arguments
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes 15)
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 3) `
    -MultipleInstances IgnoreNew -StartWhenAvailable
$principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings -Principal $principal -Force | Out-Null

$task = Get-ScheduledTask -TaskName $taskName
if ($task.State -eq "Disabled") {
    throw "$taskName was registered disabled"
}
Write-Output ("CANARY_INSTALLED task={0} state={1}" -f $taskName, $task.State)
