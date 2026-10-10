<#
.SYNOPSIS  Publishes server + client and archives them with a SHA256 file into ./artifacts.
           win-* => .zip ; linux-* => .tar.gz (keeps the executable bit).
.EXAMPLE   .\scripts\package.ps1 -Version 1.0.0
.EXAMPLE   .\scripts\package.ps1 -Version 1.0.0 -Rid linux-x64 -SelfContained
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
$extra = @("$root/README.md", "$root/LICENSE", "$root/THIRD-PARTY-NOTICES.md", "$root/docs/PRODUCTION_GUIDE.md")
if ($Rid -like "win-*")   { $extra += "$root/scripts/install-service.ps1", "$root/scripts/uninstall-service.ps1" }
if ($Rid -like "linux-*") { $extra += "$root/scripts/msmqcached.service" }
Copy-Item $extra $stage

if ($Rid -like "linux-*") {
    $archive = Join-Path $root "$Out/$name.tar.gz"
    if (Test-Path $archive) { Remove-Item $archive }
    tar -czf $archive -C $stage .
    if ($LASTEXITCODE -ne 0) { throw "tar failed" }
} else {
    $archive = Join-Path $root "$Out/$name.zip"
    if (Test-Path $archive) { Remove-Item $archive }
    Compress-Archive -Path "$stage/*" -DestinationPath $archive
}
$hash = (Get-FileHash $archive -Algorithm SHA256).Hash.ToLower()
"$hash  $(Split-Path -Leaf $archive)" | Set-Content "$archive.sha256" -Encoding ascii
Write-Host "Created $archive"
