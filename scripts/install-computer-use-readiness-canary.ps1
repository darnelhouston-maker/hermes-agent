param(
    [Parameter(Mandatory = $true)][string]$PythonExe,
    [Parameter(Mandatory = $true)][string]$ScriptPath,
    [Parameter(Mandatory = $true)][string]$HermesHome
)

$ErrorActionPreference = "Stop"
$taskName = "HermesComputerUseReadinessCanary"
$arguments = ('"{0}" --hermes-home "{1}"' -f $ScriptPath, $HermesHome)
$action = New-ScheduledTaskAction -Execute $PythonExe -Argument $arguments
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes 15)
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 3) `
    -MultipleInstances IgnoreNew -StartWhenAvailable
$principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive -RunLevel Highest
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings -Principal $principal -Force | Out-Null

$task = Get-ScheduledTask -TaskName $taskName
if ($task.State -eq "Disabled") {
    throw "$taskName was registered disabled"
}
Write-Output ("CANARY_INSTALLED task={0} state={1}" -f $taskName, $task.State)
