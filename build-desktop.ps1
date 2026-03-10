param(
    [string]$Name = "DMHelper",
    [switch]$Clean
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

if ($Clean) {
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "build"
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "dist"
    Remove-Item -Force -ErrorAction SilentlyContinue "$Name.spec"
}

uv run pyinstaller `
    --noconfirm `
    --clean `
    --onefile `
    --windowed `
    --name $Name `
    --icon "ui\static\dmhelper.ico" `
    --collect-submodules webview `
    --hidden-import uvicorn.logging `
    --hidden-import uvicorn.loops.auto `
    --hidden-import uvicorn.protocols.http.auto `
    --hidden-import uvicorn.protocols.websockets.auto `
    --add-data "ui\static;ui\static" `
    desktop_app.py

Write-Host "Build complete: dist\$Name.exe"
