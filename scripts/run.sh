#!/bin/bash

set -e

echo "=== Running Polars Go Bridge ==="

# 检测操作系统
OS=$(uname -s | tr '[:upper:]' '[:lower:]')

case "$OS" in
  linux*)
    LIB_NAME="libpolars_bridge.so"
    ;;
  darwin*)
    LIB_NAME="libpolars_bridge.dylib"
    ;;
  *)
    echo "Unsupported OS: $OS"
    exit 1
    ;;
esac

# 检查库文件是否存在
if [ ! -f "../$LIB_NAME" ]; then
    echo "Error: $LIB_NAME not found!"
    echo "Please run build.sh first."
    exit 1
fi

# 获取绝对路径
LIB_PATH="$(cd .. && pwd)/$LIB_NAME"

# 设置环境变量并运行
export POLARS_BRIDGE_LIB="$LIB_PATH"
echo "Library path: $POLARS_BRIDGE_LIB"
echo ""

# 运行 Go 程序
cd ..
go run main.go
