@echo off
setlocal

echo === Building Polars Go Bridge ===

set TARGET=x86_64-pc-windows-msvc
set LIB_NAME=polars_bridge.dll

echo Building for: %TARGET%

REM 构建 Rust 库
pushd %~dp0..\rust
cargo build --release --target %TARGET%
popd

REM 复制库文件到根目录
copy "%~dp0..\rust\target\%TARGET%\release\%LIB_NAME%" "%~dp0.."

echo.
echo ✅ Build successful!
echo Library: %CD%\%LIB_NAME%
echo.
echo To use it:
echo   set POLARS_BRIDGE_LIB=%CD%\%LIB_NAME%
echo   go run main.go
