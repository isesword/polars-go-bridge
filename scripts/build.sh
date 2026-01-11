#!/bin/bash

set -e

echo "=== Building Polars Go Bridge ==="

# 检测操作系统和架构
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$OS" in
  linux*)
    TARGET="x86_64-unknown-linux-gnu"
    LIB_NAME="libpolars_bridge.so"
    ;;
  darwin*)
    if [ "$ARCH" = "arm64" ]; then
      TARGET="aarch64-apple-darwin"
    else
      TARGET="x86_64-apple-darwin"
    fi
    LIB_NAME="libpolars_bridge.dylib"
    ;;
  *)
    echo "Unsupported OS: $OS"
    exit 1
    ;;
esac

echo "Building for: $TARGET"

# 构建 Rust 库
cd ../rust
cargo build --release --target "$TARGET"
cd ..

# 复制库文件到根目录
cp "rust/target/$TARGET/release/$LIB_NAME" .

echo ""
echo "✅ Build successful!"
echo "Library: $(pwd)/$LIB_NAME"
echo ""
echo "To use it:"
echo "  export POLARS_BRIDGE_LIB=$(pwd)/$LIB_NAME"
echo "  go run main.go"
