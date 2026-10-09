#Requires -RunAsAdministrator
param([string]$Name = "MsmqCache")
$ErrorActionPreference = "Stop"
$svc = Get-Service -Name $Name -ErrorAction SilentlyContinue
if (-not $svc) { Write-Host "Service '$Name' not found."; return }
if ($svc.Status -ne "Stopped") { Stop-Service -Name $Name -Force }
sc.exe delete $Name | Out-Null
Get-NetFirewallRule -DisplayName "$Name TCP *" -ErrorAction SilentlyContinue | Remove-NetFirewallRule
Write-Host "Service '$Name' removed. Data/config files were left in place."
