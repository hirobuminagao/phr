$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")

Set-Location $repoRoot

Write-Host "PHR Health Exam Admin"
Write-Host "Repository: $repoRoot"
Write-Host "URL       : http://127.0.0.1:8011"
Write-Host ""
Write-Host "Stop: Ctrl + C"
Write-Host ""

try {
    python -m uvicorn apps.health_exam_admin.main:app --host 127.0.0.1 --port 8011 --reload
}
catch {
    Write-Host ""
    Write-Host "Failed to start Health Exam Admin."
    Write-Host $_
    Write-Host ""
    Read-Host "Press Enter to close"
    exit 1
}
