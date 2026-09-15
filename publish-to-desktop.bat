@echo off
setlocal

set "OUTPUT_DIR=C:\Users\painu\Desktop\FoxProToMySqlMigrator"
set "VERSION_FILE=publish-version.txt"
set "VERSION_PREFIX=1.0.0"
set "BUILD_NUMBER=0"

if exist "%VERSION_FILE%" (
    set /p BUILD_NUMBER=<"%VERSION_FILE%"
)

set /a NEXT_BUILD_NUMBER=BUILD_NUMBER+1
set "APP_VERSION=%VERSION_PREFIX%.%NEXT_BUILD_NUMBER%"

echo ========================================
echo Publishing to Desktop Folder
echo ========================================
echo.
echo Version:
echo %APP_VERSION%
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
dotnet build FoxProToMySqlMigrator\FoxProToMySqlMigrator.csproj -c Release -r win-x64 --self-contained true -p:Version=%APP_VERSION% -p:AssemblyVersion=%APP_VERSION% -p:FileVersion=%APP_VERSION%

if errorlevel 1 (
    echo.
    echo Build failed.
    pause
    exit /b 1
)

echo.
echo Publishing Release...
dotnet publish FoxProToMySqlMigrator\FoxProToMySqlMigrator.csproj -c Release -r win-x64 --self-contained true -p:Version=%APP_VERSION% -p:AssemblyVersion=%APP_VERSION% -p:FileVersion=%APP_VERSION% -p:PublishSingleFile=true -p:PublishReadyToRun=true -p:IncludeNativeLibrariesForSelfExtract=true -p:DebugType=none -p:DebugSymbols=false -o "%OUTPUT_DIR%"

if errorlevel 1 (
    echo.
    echo Publish failed.
    pause
    exit /b 1
)

(
    echo FoxProToMySqlMigrator
    echo Version: %APP_VERSION%
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
echo %APP_VERSION%
echo.
pause
