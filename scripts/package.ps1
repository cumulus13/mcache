<#
.SYNOPSIS  Publishes server + client and zips them with a SHA256 file into ./artifacts.
.EXAMPLE   .\scripts\package.ps1 -Version 1.0.0
.EXAMPLE   .\scripts\package.ps1 -Version 1.0.0 -SelfContained
#>
param(
    [string]$Version = "0.0.0-local",
    [string]$Rid     = "win-x64",
    [switch]$SelfContained,
    [string]$Out     = "artifacts"
)
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$sc   = if ($SelfContained) { "true" } else { "false" }
$kind = if ($SelfContained) { "self-contained" } else { "framework-dependent" }
$name = "msmqcache-$Version-$Rid-$kind"
$stage = Join-Path $root "$Out/$name"

if (Test-Path $stage) { Remove-Item $stage -Recurse -Force }
New-Item -ItemType Directory -Path $stage | Out-Null

foreach ($p in @("Server", "Client")) {
    dotnet publish "$root/src/MsmqCache.$p/MsmqCache.$p.csproj" -c Release -r $Rid --self-contained $sc `
        -p:PublishSingleFile=true -p:DebugType=none -p:Version=$Version -o "$stage/$($p.ToLower())"
    if ($LASTEXITCODE -ne 0) { throw "publish $p failed" }
}

Copy-Item "$root/config" "$stage/config" -Recurse
Copy-Item "$root/scripts/install-service.ps1", "$root/scripts/uninstall-service.ps1", "$root/README.md", "$root/LICENSE" $stage

$zip = Join-Path $root "$Out/$name.zip"
if (Test-Path $zip) { Remove-Item $zip }
Compress-Archive -Path "$stage/*" -DestinationPath $zip
$hash = (Get-FileHash $zip -Algorithm SHA256).Hash.ToLower()
"$hash  $name.zip" | Set-Content "$zip.sha256" -Encoding ascii
Write-Host "Created $zip"
