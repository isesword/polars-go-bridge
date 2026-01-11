@echo off
set "POLARS_BRIDGE_LIB=%~dp0..\polars_bridge.dll"
go test ..\.\polars\ -v -count=1