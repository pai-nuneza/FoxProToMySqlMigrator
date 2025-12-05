@echo off
echo ========================================
echo Building Single Executable Release
echo ========================================
echo.

dotnet publish FoxProToMySqlMigrator\FoxProToMySqlMigrator.csproj -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true -p:PublishReadyToRun=true -p:IncludeNativeLibrariesForSelfExtract=true -p:DebugType=none -p:DebugSymbols=false

echo.
echo ========================================
echo Build Complete!
echo ========================================
echo.
echo Your executable is located at:
echo FoxProToMySqlMigrator\bin\Release\net10.0-windows\win-x64\publish\FoxProToMySqlMigrator.exe
echo.
echo (No .pdb file generated - smaller size)
echo.
pause
