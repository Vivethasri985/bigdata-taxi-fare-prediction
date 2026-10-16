@echo off
setlocal

:: Request Administrator privileges automatically if not elevated
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo Requesting Administrator privileges...
    powershell -Command "Start-Process cmd -ArgumentList '/k \"\"%~f0\"\"' -Verb RunAs"
    exit /b
)

echo ===================================================
echo   SurgeSense: Compacting Docker Disk (Freeing Space)
echo ===================================================
echo.

echo [1/3] Ensuring Docker Desktop and WSL are completely stopped...
taskkill /F /IM "Docker Desktop.exe" /T >nul 2>&1
taskkill /F /IM "com.docker.backend.exe" /T >nul 2>&1
taskkill /F /IM "com.docker.build.exe" /T >nul 2>&1
wsl --shutdown >nul 2>&1
timeout /t 3 /nobreak >nul

echo [2/3] Compacting virtual disk in-place on D: drive...
echo This will take 1-3 minutes. Please wait...
(
echo select vdisk file="D:\DockerData\DockerDesktopWSL\DockerDesktopWSL\disk\docker_data.vhdx"
echo attach vdisk readonly
echo compact vdisk
echo detach vdisk
echo exit
) > "%temp%\compact_commands.txt"

diskpart /s "%temp%\compact_commands.txt"
del "%temp%\compact_commands.txt" >nul 2>&1

echo.
echo [3/3] Done! Checking D: drive free space...
echo ===================================================
powershell -Command "Get-PSDrive D | Select-Object Name, @{Name='UsedGB';Expression={[math]::Round($_.Used/1GB,2)}}, @{Name='FreeGB';Expression={[math]::Round($_.Free/1GB,2)}}"
echo ===================================================

echo.
echo You can now start Docker Desktop.
pause
