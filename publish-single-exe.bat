@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "OUTPUT_DIR=%SCRIPT_DIR%publish\win-x64"
set "VERSION_FILE=%SCRIPT_DIR%publish-version.txt"
set "VERSION_PREFIX=1.0.0"
set "BUILD_NUMBER=0"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format MMddyyyy"') do set "PUBLISH_DATE=%%i"

if exist "%VERSION_FILE%" (
    set /p BUILD_NUMBER=<"%VERSION_FILE%"
)

set /a NEXT_BUILD_NUMBER=BUILD_NUMBER+1
set "APP_VERSION=%VERSION_PREFIX%.%NEXT_BUILD_NUMBER%"
set "DISPLAY_VERSION=%APP_VERSION%-%PUBLISH_DATE%"

echo ========================================
echo Building Single Executable Release
echo ========================================
echo.
echo Version:
echo %DISPLAY_VERSION%
echo.

dotnet publish "%SCRIPT_DIR%FoxProToMySqlMigrator\FoxProToMySqlMigrator.csproj" -c Release -r win-x64 --self-contained true -p:Version=%APP_VERSION% -p:AssemblyVersion=%APP_VERSION% -p:FileVersion=%APP_VERSION% -p:InformationalVersion=%DISPLAY_VERSION% -p:IncludeSourceRevisionInInformationalVersion=false -p:PublishSingleFile=true -p:PublishReadyToRun=true -p:IncludeNativeLibrariesForSelfExtract=true -p:DebugType=none -p:DebugSymbols=false -o "%OUTPUT_DIR%"

if errorlevel 1 (
    echo.
    echo Build failed.
    pause
    exit /b 1
)

if not exist "%OUTPUT_DIR%\Data" mkdir "%OUTPUT_DIR%\Data"
copy /Y "%SCRIPT_DIR%FoxProToMySqlMigrator\needed-tables.json" "%OUTPUT_DIR%\Data\needed-tables.json" >nul

if errorlevel 1 (
    echo.
    echo Failed to copy needed-tables.json.
    pause
    exit /b 1
)

(
    echo FoxProToMySqlMigrator
    echo Version: %APP_VERSION%
    echo Display Version: %DISPLAY_VERSION%
    echo Published: %DATE% %TIME%
) > "%OUTPUT_DIR%\VERSION.txt"

echo %NEXT_BUILD_NUMBER%>"%VERSION_FILE%"

echo.
echo Creating zip package...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Compress-Archive -Path '%OUTPUT_DIR%\*' -DestinationPath '%SCRIPT_DIR%publish\FoxProToMySqlMigrator-win-x64-%DISPLAY_VERSION%.zip' -Force"

echo.
echo ========================================
echo Build Complete!
echo ========================================
echo.
echo Your executable is located at:
echo %OUTPUT_DIR%\FoxProToMySqlMigrator.exe
echo.
echo Zip package:
echo %SCRIPT_DIR%publish\FoxProToMySqlMigrator-win-x64-%DISPLAY_VERSION%.zip
echo.
echo Version:
echo %DISPLAY_VERSION%
echo.
pause
