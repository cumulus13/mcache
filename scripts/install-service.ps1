#Requires -RunAsAdministrator
<#
.SYNOPSIS  Installs mcached as an auto-start Windows Service.
.PARAMETER EnableMsmq  Also installs/starts the MSMQ Windows feature (only needed if EnableMsmq=true in config.json).
.PARAMETER OpenFirewall  Adds an inbound allow rule for the TCP port (only do this with Password and/or TLS configured).
.EXAMPLE   .\install-service.ps1 -Dir C:\MsmqCache\server
#>
param(
    [string]$Dir  = (Join-Path $PSScriptRoot "server"),
    [string]$Name = "MsmqCache",
    [switch]$EnableMsmq,
    [switch]$OpenFirewall,
    [int]$Port = 6379
)
$ErrorActionPreference = "Stop"

$exe = Join-Path $Dir "mcached.exe"
if (-not (Test-Path $exe)) { throw "Not found: $exe" }

if ($EnableMsmq) {
    $feature = Get-WindowsOptionalFeature -Online -FeatureName MSMQ-Server
    if ($feature.State -ne "Enabled") {
        Write-Host "Enabling MSMQ..."
        Enable-WindowsOptionalFeature -Online -FeatureName MSMQ-Server -All -NoRestart | Out-Null
    }
}

if (Get-Service -Name $Name -ErrorAction SilentlyContinue) { throw "Service '$Name' already exists. Run uninstall-service.ps1 first." }

New-Service -Name $Name -BinaryPathName "`"$exe`"" -DisplayName "MsmqCache (mcached)" `
    -Description "Redis-compatible cache server (RESP over TCP, optional MSMQ transport)" -StartupType Automatic | Out-Null
# restart on failure: 5s, 5s, 30s
sc.exe failure $Name reset= 86400 actions= restart/5000/restart/5000/restart/30000 | Out-Null

if ($OpenFirewall) {
    New-NetFirewallRule -DisplayName "$Name TCP $Port" -Direction Inbound -Protocol TCP -LocalPort $Port -Action Allow | Out-Null
    Write-Host "Firewall rule added for TCP $Port"
}

Start-Service -Name $Name
Get-Service -Name $Name
