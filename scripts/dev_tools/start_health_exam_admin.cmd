@echo off
setlocal

set SCRIPT_DIR=%~dp0

start "PHR Health Exam Admin" powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%start_health_exam_admin.ps1"
