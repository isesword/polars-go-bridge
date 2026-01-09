@echo off
setlocal

echo === Running Polars Go Bridge ===

REM 检查 DLL 是否存在
set LIB_NAME=polars_bridge.dll
if not exist "..\%LIB_NAME%" (
    echo Error: %LIB_NAME% not found!
    echo Please run build.bat first.
    exit /b 1
)

REM 设置环境变量
set POLARS_BRIDGE_LIB=%CD%\..\%LIB_NAME%
echo Library path: %POLARS_BRIDGE_LIB%
echo.

REM 运行 Go 程序
cd ..
go run main.go

endlocal
