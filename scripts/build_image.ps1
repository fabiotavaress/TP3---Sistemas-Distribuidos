# Constroi a imagem unica do TP3 (Windows / PowerShell)
# Uso:  .\scripts\build_image.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
docker build -t tp3-app:latest .
Write-Host "`nImagem tp3-app:latest construida com sucesso." -ForegroundColor Green
