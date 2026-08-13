$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$port = 8011

Set-Location $repoRoot

function Get-HealthExamAdminProcesses {
    $processes = @()
    try {
        $processes += Get-CimInstance Win32_Process |
            Where-Object {
                $_.CommandLine -and (
                    $_.CommandLine -like "*apps.health_exam_admin.main:app*" -or
                    $_.CommandLine -like "*start_health_exam_admin.ps1*"
                ) -and $_.ProcessId -ne $PID
            }
    }
    catch {
        Write-Host "Could not inspect existing Health Exam Admin processes."
    }

    try {
        $listening = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
        foreach ($conn in $listening) {
            if ($conn.OwningProcess -and $conn.OwningProcess -ne $PID) {
                $process = Get-CimInstance Win32_Process -Filter "ProcessId = $($conn.OwningProcess)" -ErrorAction SilentlyContinue
                if ($process) {
                    $processes += $process
                }
            }
        }
    }
    catch {
        Write-Host "Could not inspect port $port."
    }

    $processes |
        Where-Object { $_ -and $_.ProcessId -and $_.ProcessId -ne $PID } |
        Sort-Object ProcessId -Unique
}

function Wait-PortFree {
    param([int]$Port, [int]$Seconds = 8)

    for ($i = 0; $i -lt $Seconds; $i++) {
        $listening = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
        if (-not $listening) {
            return $true
        }
        Start-Sleep -Seconds 1
    }
    return $false
}

$existingProcesses = @(Get-HealthExamAdminProcesses)
if ($existingProcesses.Count -gt 0) {
    Write-Host "Health Exam Admin appears to be already running."
    Write-Host ""
    foreach ($process in $existingProcesses) {
        $commandLine = $process.CommandLine
        if ($commandLine -and $commandLine.Length -gt 120) {
            $commandLine = $commandLine.Substring(0, 120) + "..."
        }
        Write-Host ("PID {0}: {1}" -f $process.ProcessId, ($commandLine -or $process.Name))
    }
    Write-Host ""
    $answer = Read-Host "Restart it? (Y/N)"
    if ($answer -notin @("Y", "y", "YES", "yes")) {
        Write-Host "Canceled. Existing Health Exam Admin was left running."
        Read-Host "Press Enter to close"
        exit 0
    }

    foreach ($process in $existingProcesses) {
        try {
            Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
            Write-Host ("Stopped PID {0}" -f $process.ProcessId)
        }
        catch {
            Write-Host ("Could not stop PID {0}: {1}" -f $process.ProcessId, $_)
        }
    }

    if (-not (Wait-PortFree -Port $port)) {
        Write-Host ""
        Write-Host "Port $port is still in use. Please close the old terminal or process, then run this shortcut again."
        Read-Host "Press Enter to close"
        exit 1
    }
    Write-Host ""
}

Write-Host "PHR Health Exam Admin"
Write-Host "Repository: $repoRoot"
Write-Host "URL       : http://127.0.0.1:$port"
Write-Host ""
Write-Host "Stop: Ctrl + C"
Write-Host ""

try {
    python -m uvicorn apps.health_exam_admin.main:app --host 127.0.0.1 --port $port --reload
}
catch {
    Write-Host ""
    Write-Host "Failed to start Health Exam Admin."
    Write-Host $_
    Write-Host ""
    Read-Host "Press Enter to close"
    exit 1
}
