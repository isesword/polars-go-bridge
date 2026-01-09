# Polars Go Bridge

跨平台 Go-Polars Bridge 最小基础版，支持 Windows、Linux 和 macOS。

## 特性

- ✅ 跨平台支持（Windows/Linux/macOS，x64 和 ARM64）
- ✅ 免 CGO：Go 侧使用 PureGo 绑定
- ✅ Protobuf Plan：可演进的计划协议
- ✅ 简化数据交换：使用 JSON 格式（适合原型阶段）
- ✅ 线程安全的错误处理

## 架构

```
Go Application
    ↓ (JSON Data + Protobuf Plan)
Go Bridge Loader (PureGo FFI)
    ↓ (C ABI)
Rust cdylib (Polars Engine)
    ↓
Polars DataFrame Operations
```

## 支持的操作（v0.1）

### 节点类型
- **MemoryScan**: 从输入数据扫描
- **Project**: 选择列
- **Filter**: 过滤行
- **WithColumns**: 添加/修改列
- **Limit**: 限制行数

### 表达式
- **Column**: 列引用 (`col("name")`)
- **Literal**: 字面量（int64, float64, bool, string, null）
- **Binary**: 二元操作（+, -, *, /, ==, !=, <, <=, >, >=, AND, OR）
- **Alias**: 别名
- **IsNull**: 空值检查

## 快速开始

### 1. 安装依赖

#### Rust 侧
```bash
cd rust
cargo build --release
```

#### Go 侧
```bash
go mod download
```

### 2. 设置库路径

将构建好的动态库放到可执行文件目录，或设置环境变量：

**Linux/macOS:**
```bash
export POLARS_BRIDGE_LIB=/path/to/libpolars_bridge.so  # 或 .dylib
```

**Windows:**
```powershell
$env:POLARS_BRIDGE_LIB="C:\path\to\polars_bridge.dll"
```

### 3. 运行示例

```bash
go run main.go
```

## 构建

### 构建 Rust 动态库

**Windows (MSVC):**
```bash
cd rust
cargo build --release --target x86_64-pc-windows-msvc
```

**Linux:**
```bash
cd rust
cargo build --release --target x86_64-unknown-linux-gnu
```

**macOS (Intel):**
```bash
cd rust
cargo build --release --target x86_64-apple-darwin
```

**macOS (Apple Silicon):**
```bash
cd rust
cargo build --release --target aarch64-apple-darwin
```

构建产物位于 `rust/target/<target>/release/`。

### 多平台构建

项目包含 GitHub Actions 配置，自动构建所有平台的动态库：
- Windows x64 (MSVC)
- Linux x64 (glibc)
- macOS x64
- macOS ARM64

## 使用示例

```go
package main

import (
    "fmt"
    "log"
    "github.com/polars-go-bridge/bridge"
)

func main() {
    // 加载 Bridge
    brg, err := bridge.LoadBridge("")
    if err != nil {
        log.Fatal(err)
    }

    // 获取版本信息
    fmt.Printf("ABI Version: %d\n", brg.AbiVersion())
    
    engineVer, _ := brg.EngineVersion()
    fmt.Printf("Engine Version: %s\n", engineVer)
    
    caps, _ := brg.Capabilities()
    fmt.Printf("Capabilities: %s\n", caps)
}
```

## 错误码

| Code | 名称 | 说明 |
|------|------|------|
| 0 | OK | 成功 |
| 1 | ERR_UNKNOWN | 未知错误 |
| 2 | ERR_INVALID_ARGUMENT | 参数非法 |
| 3 | ERR_ABI_MISMATCH | ABI 版本不匹配 |
| 4 | ERR_PLAN_VERSION_UNSUPPORTED | Plan 版本不支持 |
| 5 | ERR_PLAN_DECODE | Protobuf 解码失败 |
| 6 | ERR_PLAN_SEMANTIC | 计划语义错误 |
| 7 | ERR_ARROW_IMPORT | Arrow 导入失败 |
| 8 | ERR_ARROW_EXPORT | Arrow 导出失败 |
| 9 | ERR_EXECUTION | 执行失败 |
| 10 | ERR_UNSUPPORTED | 功能不支持 |
| 11 | ERR_OOM | 内存不足 |

## 兼容性

### 最低要求

- **Go**: 1.21+
- **Rust**: 1.70+
- **Linux**: glibc 2.17+ (CentOS 7+, Ubuntu 16.04+)
- **macOS**: 10.13+
- **Windows**: Windows 10+ (需要 MSVC Runtime)

### 已测试平台

- ✅ Windows 11 x64
- ✅ Ubuntu 22.04 x64
- ✅ macOS 13+ (Intel & Apple Silicon)

## 开发路线图

### v0.1 (当前) - 最小可行版本
- [x] 基础 FFI 接口
- [x] Protobuf Plan v1
- [x] 简单 JSON 数据交换
- [x] 基本操作：Scan, Project, Filter, WithColumns, Limit
- [x] 多平台构建

### v0.2 - 性能优化
- [ ] Arrow C Data Interface
- [ ] 零拷贝数据传输
- [ ] 更多操作：GroupBy, Sort, Join
- [ ] 性能基准测试

### v0.3 - 生产就绪
- [ ] 完整测试套件
- [ ] 文档完善
- [ ] 错误处理增强
- [ ] 内存安全审计

## 故障排查

### 加载库失败

**问题**: `library not found`

**解决**:
1. 确认动态库已构建并存在
2. 设置 `POLARS_BRIDGE_LIB` 环境变量为绝对路径
3. 或将库放到可执行文件同目录

### ABI 版本不匹配

**问题**: `ABI version mismatch`

**解决**:
- 确保 Go 代码和 Rust 库版本一致
- 重新构建 Rust 库
- 清理并重新编译

### Linux 上的 glibc 问题

**问题**: `version 'GLIBC_X.XX' not found`

**解决**:
- 使用较老的 Linux 发行版构建
- 或使用 musl 静态链接（需修改 Rust target）

### macOS Gatekeeper

**问题**: `"libpolars_bridge.dylib" cannot be opened`

**解决**:
```bash
xattr -d com.apple.quarantine libpolars_bridge.dylib
```

## 贡献

欢迎贡献！请：
1. Fork 项目
2. 创建特性分支
3. 提交 PR

## 许可证

MIT License

## 参考

- [Polars](https://github.com/pola-rs/polars)
- [Apache Arrow](https://arrow.apache.org/)
- [Protocol Buffers](https://protobuf.dev/)
- [PureGo](https://github.com/ebitengine/purego)
