$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
$envPath = Join-Path $repoRoot "scripts\.env"

function Get-DotEnvValue {
    param([string]$Path, [string]$Key)

    if (-not (Test-Path $Path)) {
        return $null
    }
    foreach ($line in Get-Content -Path $Path -Encoding UTF8) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#") -or -not $trimmed.Contains("=")) {
            continue
        }
        $parts = $trimmed.Split("=", 2)
        if ($parts[0].Trim() -eq $Key) {
            return $parts[1].Trim().Trim('"').Trim("'")
        }
    }
    return $null
}

$hostName = $env:PHR_ADMIN_HOST
if ([string]::IsNullOrWhiteSpace($hostName)) {
    $hostName = Get-DotEnvValue -Path $envPath -Key "PHR_ADMIN_HOST"
}
if ([string]::IsNullOrWhiteSpace($hostName)) {
    $hostName = "0.0.0.0"
}
$port = 8011
$runDir = Join-Path $repoRoot ".run"
$pidFile = Join-Path $runDir "health_exam_admin_launcher.pid"

Set-Location $repoRoot
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

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

function Stop-ProcessTree {
    param([int]$ProcessId)

    if (-not $ProcessId -or $ProcessId -eq $PID) {
        return
    }
    try {
        $null = & taskkill.exe /PID $ProcessId /T /F 2>&1
        Write-Host ("Stopped process tree PID {0}" -f $ProcessId)
    }
    catch {
        try {
            Stop-Process -Id $ProcessId -Force -ErrorAction Stop
            Write-Host ("Stopped PID {0}" -f $ProcessId)
        }
        catch {
            Write-Host ("Could not stop PID {0}: {1}" -f $ProcessId, $_)
        }
    }
}

function Stop-PortOwners {
    param([int]$Port)

    try {
        $listening = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
        foreach ($conn in $listening) {
            if ($conn.OwningProcess -and $conn.OwningProcess -ne $PID) {
                $process = Get-CimInstance Win32_Process -Filter "ProcessId = $($conn.OwningProcess)" -ErrorAction SilentlyContinue
                if ($process -and $process.CommandLine -and (
                    $process.CommandLine -like "*apps.health_exam_admin.main:app*" -or
                    $process.CommandLine -like "*start_health_exam_admin.ps1*"
                )) {
                    Stop-ProcessTree -ProcessId $conn.OwningProcess
                }
                else {
                    Write-Host ("Port {0} is owned by PID {1}, but it does not look like Health Exam Admin. It was not stopped." -f $Port, $conn.OwningProcess)
                }
            }
        }
    }
    catch {
        Write-Host "Could not stop port $Port owner."
    }
}

function Get-SavedLauncherProcess {
    if (-not (Test-Path $pidFile)) {
        return $null
    }
    try {
        $savedPid = [int]((Get-Content $pidFile -ErrorAction Stop | Select-Object -First 1).Trim())
        if ($savedPid -eq $PID) {
            return $null
        }
        return Get-CimInstance Win32_Process -Filter "ProcessId = $savedPid" -ErrorAction SilentlyContinue
    }
    catch {
        return $null
    }
}

$savedLauncher = Get-SavedLauncherProcess
$savedLauncherLooksActive = $savedLauncher -and $savedLauncher.CommandLine -and (
    $savedLauncher.CommandLine -like "*start_health_exam_admin.ps1*" -or
    $savedLauncher.CommandLine -like "*apps.health_exam_admin.main:app*"
)

$portIsUsed = $false
try {
    $portIsUsed = [bool](Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)
}
catch {
    $portIsUsed = $false
}

$savedExistingProcesses = @()
if ($savedLauncherLooksActive) {
    $savedExistingProcesses += $savedLauncher
}

$existingProcesses = @(Get-HealthExamAdminProcesses)
$existingProcesses = @(($existingProcesses + $savedExistingProcesses) | Where-Object { $_ -and $_.ProcessId -and $_.ProcessId -ne $PID } | Sort-Object ProcessId -Unique)
if ($existingProcesses.Count -gt 0 -or $portIsUsed) {
    Write-Host "Health Exam Admin appears to be already running."
    Write-Host ""
    foreach ($process in $existingProcesses) {
        $commandLine = $process.CommandLine
        if ($commandLine -and $commandLine.Length -gt 120) {
            $commandLine = $commandLine.Substring(0, 120) + "..."
        }
        Write-Host ("PID {0}: {1}" -f $process.ProcessId, ($commandLine -or $process.Name))
    }
    if ($existingProcesses.Count -eq 0 -and $portIsUsed) {
        Write-Host "Port $port is already in use."
    }
    Write-Host ""
    $answer = Read-Host "Restart it? (Y/N)"
    if ($answer -notin @("Y", "y", "YES", "yes")) {
        Write-Host "Canceled. Existing Health Exam Admin was left running."
        Read-Host "Press Enter to close"
        exit 0
    }

    foreach ($process in $existingProcesses) {
        Stop-ProcessTree -ProcessId $process.ProcessId
    }

    if (-not (Wait-PortFree -Port $port -Seconds 3)) {
        Stop-PortOwners -Port $port
    }

    if (-not (Wait-PortFree -Port $port)) {
        Write-Host ""
        Write-Host "Port $port is still in use. The old terminal or process could not be closed automatically."
        Read-Host "Press Enter to close"
        exit 1
    }
    Write-Host ""
}

Set-Content -Path $pidFile -Value $PID -Encoding ascii

Write-Host "PHR Health Exam Admin"
Write-Host "Repository: $repoRoot"
Write-Host "URL       : http://127.0.0.1:$port"
if ($hostName -eq "0.0.0.0") {
    Write-Host "LAN URL   : http://<this PC IP>:$port"
}
Write-Host "Bind Host : $hostName"
Write-Host ""
Write-Host "Stop: Ctrl + C"
Write-Host ""

try {
    python -m uvicorn apps.health_exam_admin.main:app --host $hostName --port $port --reload
}
catch {
    Write-Host ""
    Write-Host "Failed to start Health Exam Admin."
    Write-Host $_
    Write-Host ""
    Read-Host "Press Enter to close"
    exit 1
}
finally {
    try {
        if (Test-Path $pidFile) {
            $savedPid = (Get-Content $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
            if ([string]$savedPid -eq [string]$PID) {
                Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
            }
        }
    }
    catch {
    }
}
