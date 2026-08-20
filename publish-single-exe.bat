@echo off
echo ========================================
echo Building Single Executable Release
echo ========================================
echo.

dotnet publish FoxProToMySqlMigrator\FoxProToMySqlMigrator.csproj -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true -p:PublishReadyToRun=true -p:IncludeNativeLibrariesForSelfExtract=true -p:DebugType=none -p:DebugSymbols=false -o publish\win-x64

if errorlevel 1 (
    echo.
    echo Build failed.
    pause
    exit /b 1
)

echo.
echo Creating zip package...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Compress-Archive -Path 'publish\win-x64\*' -DestinationPath 'publish\FoxProToMySqlMigrator-win-x64.zip' -Force"

echo.
echo ========================================
echo Build Complete!
echo ========================================
echo.
echo Your executable is located at:
echo publish\win-x64\FoxProToMySqlMigrator.exe
echo.
echo Zip package:
echo publish\FoxProToMySqlMigrator-win-x64.zip
echo.
pause
