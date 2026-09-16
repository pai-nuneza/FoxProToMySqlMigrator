@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "OUTPUT_DIR=C:\Users\painu\Desktop\FoxProToMySqlMigrator"
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
echo Publishing to Desktop Folder
echo ========================================
echo.
echo Version:
echo %DISPLAY_VERSION%
echo.
echo Output:
echo %OUTPUT_DIR%
echo.

echo Cleaning Release build...
dotnet clean FoxProToMySqlMigrator\FoxProToMySqlMigrator.csproj -c Release

if errorlevel 1 (
    echo.
    echo Clean failed.
    pause
    exit /b 1
)

echo.
echo Building Release...
dotnet build "%SCRIPT_DIR%FoxProToMySqlMigrator\FoxProToMySqlMigrator.csproj" -c Release -r win-x64 --self-contained true -p:Version=%APP_VERSION% -p:AssemblyVersion=%APP_VERSION% -p:FileVersion=%APP_VERSION% -p:InformationalVersion=%DISPLAY_VERSION% -p:IncludeSourceRevisionInInformationalVersion=false

if errorlevel 1 (
    echo.
    echo Build failed.
    pause
    exit /b 1
)

echo.
echo Publishing Release...
dotnet publish "%SCRIPT_DIR%FoxProToMySqlMigrator\FoxProToMySqlMigrator.csproj" -c Release -r win-x64 --self-contained true -p:Version=%APP_VERSION% -p:AssemblyVersion=%APP_VERSION% -p:FileVersion=%APP_VERSION% -p:InformationalVersion=%DISPLAY_VERSION% -p:IncludeSourceRevisionInInformationalVersion=false -p:PublishSingleFile=true -p:PublishReadyToRun=true -p:IncludeNativeLibrariesForSelfExtract=true -p:DebugType=none -p:DebugSymbols=false -o "%OUTPUT_DIR%"

if errorlevel 1 (
    echo.
    echo Publish failed.
    pause
    exit /b 1
)

if not exist "%OUTPUT_DIR%\Data" mkdir "%OUTPUT_DIR%\Data"
if not exist "%OUTPUT_DIR%\Data\needed-tables.json" (
    copy "%SCRIPT_DIR%FoxProToMySqlMigrator\needed-tables.json" "%OUTPUT_DIR%\Data\needed-tables.json" >nul
)

(
    echo FoxProToMySqlMigrator
    echo Version: %APP_VERSION%
    echo Display Version: %DISPLAY_VERSION%
    echo Published: %DATE% %TIME%
) > "%OUTPUT_DIR%\VERSION.txt"

echo %NEXT_BUILD_NUMBER%>"%VERSION_FILE%"

echo.
echo ========================================
echo Publish Complete!
echo ========================================
echo.
echo Your executable is located at:
echo %OUTPUT_DIR%\FoxProToMySqlMigrator.exe
echo.
echo Version:
echo %DISPLAY_VERSION%
echo.
pause
