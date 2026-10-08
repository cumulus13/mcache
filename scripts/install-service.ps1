#Requires -RunAsAdministrator
<#
.SYNOPSIS  Installs MsmqCache as an auto-start Windows Service (enables MSMQ if needed).
.EXAMPLE   .\install-service.ps1 -Dir C:\MsmqCache\server
#>
param(
    [string]$Dir  = (Join-Path $PSScriptRoot "server"),
    [string]$Name = "MsmqCache"
)
$ErrorActionPreference = "Stop"

$exe = Join-Path $Dir "mcached.exe"
if (-not (Test-Path $exe)) { throw "Not found: $exe" }

$feature = Get-WindowsOptionalFeature -Online -FeatureName MSMQ-Server
if ($feature.State -ne "Enabled") {
    Write-Host "Enabling MSMQ..."
    Enable-WindowsOptionalFeature -Online -FeatureName MSMQ-Server -All -NoRestart | Out-Null
}

if (Get-Service -Name $Name -ErrorAction SilentlyContinue) { throw "Service '$Name' already exists. Run uninstall-service.ps1 first." }

New-Service -Name $Name -BinaryPathName "`"$exe`"" -DisplayName "MSMQ Cache Server" `
    -Description "Redis-like cache server over MSMQ" -StartupType Automatic | Out-Null
# restart on failure: 5s, 5s, 30s
sc.exe failure $Name reset= 86400 actions= restart/5000/restart/5000/restart/30000 | Out-Null
Start-Service -Name $Name
Get-Service -Name $Name
