# 项目结构

```
polars-go-bridge/
├── .github/
│   └── workflows/
│       └── build.yml              # CI/CD 多平台构建配置
│
├── bridge/                        # Go Bridge 包
│   ├── loader.go                  # Windows FFI 加载器
│   ├── loader_unix.go             # Unix/Linux/macOS FFI 加载器
│   └── bridge_test.go             # 单元测试
│
├── proto/                         # Protobuf 定义
│   └── plan.proto                 # Plan v1 schema
│
├── rust/                          # Rust 引擎
│   ├── src/
│   │   ├── lib.rs                 # FFI 导出函数
│   │   ├── error.rs               # 错误码和错误类型
│   │   └── executor.rs            # Plan 执行器
│   ├── Cargo.toml                 # Rust 依赖配置
│   └── build.rs                   # Protobuf 构建脚本
│
├── .gitignore                     # Git 忽略文件
├── build.sh                       # Unix 构建脚本
├── build.bat                      # Windows 构建脚本
├── go.mod                         # Go 模块定义
├── main.go                        # 示例程序
├── README.md                      # 项目说明
├── QUICKSTART.md                  # 快速开始指南
└── todo.md                        # 设计文档（v0.3）
```

## 核心组件说明

### 1. Protobuf Plan (`proto/`)
- **plan.proto**: 定义执行计划的数据结构
  - Node: MemoryScan, Project, Filter, WithColumns, Limit
  - Expr: Column, Literal, Binary, Alias, IsNull
  - 预留字段号用于未来扩展

### 2. Rust Engine (`rust/`)
- **lib.rs**: C ABI 导出
  - `bridge_abi_version()`: ABI 版本
  - `bridge_engine_version()`: 引擎版本
  - `bridge_capabilities()`: 能力协商
  - `bridge_last_error()`: 线程局部错误
  - `bridge_plan_compile()`: 编译 Plan
  - `bridge_plan_execute_simple()`: 执行 Plan (JSON I/O)
  
- **error.rs**: 错误处理
  - 11 个错误码
  - 安全的 panic 捕获
  
- **executor.rs**: 执行引擎
  - Plan 解释器
  - Polars DataFrame 操作
  - JSON 序列化/反序列化

### 3. Go Bridge (`bridge/`)
- **loader.go / loader_unix.go**: 动态库加载
  - Windows: syscall.LoadDLL
  - Unix: purego.Dlopen
  - 自动检测平台和架构
  - 环境变量优先级

- **bridge_test.go**: 测试套件
  - 加载测试
  - 版本协商测试
  - 并发安全测试
  - 错误处理测试

### 4. CI/CD (`.github/workflows/`)
- 4 平台自动构建
  - Windows x64 (MSVC)
  - Linux x64 (GNU)
  - macOS x64
  - macOS ARM64
- 自动测试
- 产物上传

## 构建流程

### Rust 侧
1. `build.rs` 运行 prost-build 编译 `.proto` → Rust 代码
2. 生成的代码放在 `rust/src/proto/`
3. `cargo build --release` 编译为 cdylib

### Go 侧
1. 不需要 protoc（仅 Rust 侧需要）
2. `LoadBridge()` 动态加载 Rust 库
3. FFI 调用 Rust 导出的 C 函数

## 数据流

```
Go App
  ↓ JSON + Protobuf Plan bytes
Bridge Loader
  ↓ C ABI call
Rust cdylib
  ↓ Protobuf decode
Executor
  ↓ Polars operations
DataFrame
  ↓ JSON serialize
  ↑ return
Go App
```

## 平台差异处理

### Windows
- 使用 `syscall.LoadDLL`
- 库名：`polars_bridge.dll`
- Target: `x86_64-pc-windows-msvc`

### Linux
- 使用 `purego.Dlopen`
- 库名：`libpolars_bridge.so`
- Target: `x86_64-unknown-linux-gnu`
- 注意 glibc 版本

### macOS
- 使用 `purego.Dlopen`
- 库名：`libpolars_bridge.dylib`
- Target: `x86_64-apple-darwin` 或 `aarch64-apple-darwin`
- 可能需要处理 Gatekeeper

## 扩展点

### 添加新节点类型
1. 修改 `proto/plan.proto`
2. 更新 `rust/src/executor.rs` 的 `execute_node()`
3. 更新 `capabilities` 中的 `supported_nodes`

### 添加新表达式
1. 修改 `proto/plan.proto`
2. 更新 `rust/src/executor.rs` 的 `build_expr()`
3. 更新 `capabilities` 中的 `supported_exprs`

### 添加 Plan Builder (未来)
在 `bridge/` 中创建 `builder.go`：
- 提供 DSL API
- 构建 Protobuf Plan
- 类型检查

## 版本兼容性

- **ABI Version**: 当前为 1，破坏性改变时递增
- **Plan Version**: 当前为 1，字段号预留确保向前兼容
- **Engine Version**: 语义化版本 (当前 0.1.0)
