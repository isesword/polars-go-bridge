# 快速开始指南

本指南将帮助你在 5 分钟内运行 Polars Go Bridge。

## 前置要求

- **Rust** 1.70+ (`rustc --version`)
- **Go** 1.21+ (`go version`)
- **Protobuf 编译器** (可选，用于修改 proto 文件)

## Windows 构建

### 1. 安装 Rust
```powershell
# 从 https://rustup.rs/ 下载并安装
# 或使用 winget
winget install Rustlang.Rustup
```

### 2. 构建项目
```powershell
# 克隆/进入项目目录
cd polars-go-bridge

# 运行构建脚本
.\build.bat

# 设置环境变量
$env:POLARS_BRIDGE_LIB="$PWD\polars_bridge.dll"

# 运行示例
go run main.go
```

## Linux 构建

### 1. 安装依赖
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y build-essential curl

# 安装 Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

### 2. 构建项目
```bash
# 克隆/进入项目目录
cd polars-go-bridge

# 运行构建脚本
chmod +x build.sh
./build.sh

# 设置环境变量
export POLARS_BRIDGE_LIB="$(pwd)/libpolars_bridge.so"

# 运行示例
go run main.go
```

## macOS 构建

### 1. 安装依赖
```bash
# 安装 Homebrew (如果还没有)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 安装 Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

### 2. 构建项目
```bash
# 克隆/进入项目目录
cd polars-go-bridge

# 运行构建脚本
chmod +x build.sh
./build.sh

# 设置环境变量
export POLARS_BRIDGE_LIB="$(pwd)/libpolars_bridge.dylib"

# 运行示例
go run main.go
```

## 验证安装

运行测试：

```bash
# Unix (Linux/macOS)
export POLARS_BRIDGE_LIB="$(pwd)/libpolars_bridge.so"  # 或 .dylib
go test ./bridge -v

# Windows
$env:POLARS_BRIDGE_LIB="$PWD\polars_bridge.dll"
go test .\bridge -v
```

## 预期输出

如果一切正常，你应该看到：

```
ABI Version: 1
Engine Version: 0.1.0
Capabilities:
{
    "abi_version": 1,
    "min_plan_version_supported": 1,
    "max_plan_version_supported": 1,
    "supported_nodes": ["MemoryScan", "Project", "Filter", "WithColumns", "Limit"],
    "supported_exprs": ["Col", "Lit", "Binary", "Alias", "IsNull"],
    "supported_dtypes": ["Int64", "Float64", "Bool", "Utf8"],
    "execution_modes": ["collect"],
    "copy_behavior": "copy_on_boundary"
}

Bridge loaded successfully!
```

## 常见问题

### Q: Rust 编译很慢？
A: 首次编译需要下载并构建 Polars 及其依赖，可能需要 5-10 分钟。后续增量编译会快很多。

### Q: Windows 上找不到 MSVC？
A: 安装 Visual Studio Build Tools 或完整的 Visual Studio，并确保选中 C++ 工作负载。

### Q: macOS 上库加载失败？
A: 运行 `xattr -d com.apple.quarantine libpolars_bridge.dylib` 移除隔离属性。

### Q: Go 找不到模块？
A: 运行 `go mod download` 下载依赖。

## 下一步

- 查看 [README.md](README.md) 了解完整功能
- 阅读 [todo.md](todo.md) 了解架构设计
- 查看 `bridge/` 目录了解 API 使用

## 需要帮助？

如果遇到问题：
1. 检查 Rust 和 Go 版本是否满足要求
2. 确认库路径环境变量设置正确
3. 查看构建输出的错误信息
4. 提交 Issue 并附上详细信息
