````md
# 跨平台 Go-Polars Bridge（PureGo + Rust Polars + Protobuf Plan + Arrow Data）项目文档 v0.3

> 版本：v0.3（在 v0.2 基础上补齐：验收用例清单、Plan 字段号治理、错误码表、Arrow 所有权/释放规范、Rust FFI 规范文本、CI 产物与兼容矩阵）  
> 状态：Draft（可用于立项评审与任务拆解）

---

## 0. 术语与范围

- **Go SDK**：用户在 Go 中调用的 API、Loader、Plan Builder、Arrow 封装。
- **Rust Engine**：动态库（cdylib），内部使用 Polars 执行。
- **控制面（Control Plane）**：Protobuf Plan/IR，描述“要做什么”。
- **数据面（Data Plane）**：Arrow 数据（Array/Schema/ArrayStream 或 IPC），描述“数据是什么”。
- **ABI**：Rust 导出的 C ABI（PureGo 绑定的函数集合）。
- **FFI 边界**：Go↔Rust 调用边界，禁止跨边界 unwind，禁止复杂结构体直接传递。

---

## 1. 目标与非目标（重申）

### 1.1 Goals（v0.3 范围内必须落地）
1. 跨平台一致性：Win/Lin/Mac 同一套 Go 调用链工作。
2. 免 CGO：用户侧 `go build` 不依赖 C 工具链。
3. 可演进协议：Protobuf Plan 版本化；引擎暴露能力协商；Go/Rust 版本错配可预期失败。
4. 数据面优先零拷贝：满足约束时使用 Arrow C Data Interface；否则可降级 Arrow IPC（由配置控制）。

### 1.2 Non-Goals（明确不做）
- 不保证覆盖 Polars 全算子/全函数（仅子集）。
- 不提供分布式执行。
- 不支持“任意第三方 Arrow 实现的无约束互操作”——必须遵循本规范的所有权与释放约定。

---

## 2. 总体架构（v0.3 约束版）

### 2.1 统一数据通道（Single Data Channel）
- **首选**：Arrow C Data Interface（`ArrowArrayStream`）
- **降级**：Arrow IPC（`bytes` 形式，Rust 解码为 Arrow/Polars）

> 说明：v0.3 要求至少实现 C Data Interface 的基础类型互操作；IPC 作为 feature flag（可选实现）。

### 2.2 统一执行入口（Single Execution Entry）
- 输入：`(PlanBytes, InputStream)`  
- 输出：`OutputStream`  
- 资源：`PlanHandle`、`StreamHandle`（或直接以 ArrowArrayStream 传递）

---

## 3. Rust C ABI（FFI）规范（v0.3 强制）

### 3.1 函数集合（规范化名称）
建议导出以下函数（名称可调整，但语义不可缺）：

1. **版本与能力**
- `bridge_abi_version() -> u32`
- `bridge_engine_version(ptr_out, len_out) -> i32`（返回 UTF-8 字符串；或通过 last_error 通道返回）
- `bridge_capabilities(ptr_out, len_out) -> i32`（返回 JSON/Protobuf bytes，包含支持算子、表达式、dtype、执行模式等）

2. **错误通道**
- `bridge_last_error(ptr_out, len_out) -> i32`
- `bridge_last_error_free(ptr, len) -> void`

3. **Plan 生命周期**
- `bridge_plan_compile(plan_bytes_ptr, plan_bytes_len, out_plan_handle_ptr) -> i32`
- `bridge_plan_free(plan_handle) -> void`

4. **执行**
- `bridge_plan_execute(plan_handle, in_stream_ptr, out_stream_ptr) -> i32`
  - `in_stream_ptr`：指向 `ArrowArrayStream`（输入数据）
  - `out_stream_ptr`：指向 `ArrowArrayStream`（输出数据；由 Rust 初始化并填充）

> v0.3 强制：所有函数返回 `i32 status`；`0` 表示 OK；非 0 表示失败，错误详情通过 `bridge_last_error` 获取。

### 3.2 Rust 侧 FFI 安全约束（强制）
- **禁止 panic unwind 跨 FFI**：所有导出函数必须 `catch_unwind`，并将 panic 转换为错误码与 last_error。
- **禁止将 Rust 所有权对象跨 FFI 暴露**：只通过句柄（u64 或 *mut c_void）表示资源。
- **禁止返回结构体**：只返回整数状态/句柄，通过 out 参数返回。

### 3.3 线程安全与重入（v0.3 要求）
- `bridge_last_error` 必须 **线程局部（TLS）**，保证并发调用不会串话。
- `bridge_plan_execute` 允许并发执行（至少不崩溃）；若内部 Polars/全局状态不支持并发，必须在文档中声明并在 `capabilities` 中暴露 `max_concurrency`.

---

## 4. 错误码表（Error Code Table）

所有导出函数返回 `i32`，约定如下：

| Code | 名称 | 含义 | 建议处理 |
|---:|---|---|---|
| 0 | OK | 成功 | - |
| 1 | ERR_UNKNOWN | 未分类错误 | 打印 last_error，建议用户升级 |
| 2 | ERR_INVALID_ARGUMENT | 参数非法（空指针、长度非法、句柄无效） | 调用方修正 |
| 3 | ERR_ABI_MISMATCH | ABI 版本不匹配 | 阻止运行，提示升级库/包 |
| 4 | ERR_PLAN_VERSION_UNSUPPORTED | Plan 版本不支持 | 提示升级 Go SDK 或 Rust Engine |
| 5 | ERR_PLAN_DECODE | Protobuf 解码失败 | 检查 plan bytes 与版本 |
| 6 | ERR_PLAN_SEMANTIC | 计划语义错误（列不存在、类型不匹配等） | 返回可读错误 |
| 7 | ERR_ARROW_IMPORT | Arrow 输入导入失败 | 检查 Arrow stream 有效性/生命周期 |
| 8 | ERR_ARROW_EXPORT | Arrow 输出导出失败 | 报错并清理资源 |
| 9 | ERR_EXECUTION | 执行失败（Polars 内部） | 返回 Polars 错误文本 |
| 10 | ERR_UNSUPPORTED | 功能不支持（capabilities 未声明） | 提示降级或改写计划 |
| 11 | ERR_OOM | 内存不足 | 可建议降低 batch / 关闭某些优化 |

> v0.3 要求：任何非 0 返回都必须能通过 `bridge_last_error` 获取到 UTF-8 文本（不为空）。

---

## 5. Arrow 数据面互操作规范（Ownership & Lifetime）

### 5.1 总原则
- **谁分配、谁释放**：`ArrowArray` / `ArrowSchema` / `ArrowArrayStream` 的 `release` 回调负责释放其内部资源。
- **release 只调用一次**：调用后指针对象应被视为无效。
- **不允许隐式共享 Go 堆对象给 Rust**：Go 侧传给 Rust 的 Arrow 缓冲必须在 Rust 使用期间保持存活（见 5.4）。

### 5.2 数据面接口形态（v0.3 强制）
- 输入：Go 将数据以 `ArrowArrayStream` 形式提供给 Rust（可由 Go 侧 Arrow 库构造 stream）。
- 输出：Rust 填充一个 `ArrowArrayStream` 供 Go 侧消费。该 stream 的 `release` 由 Go 侧最终调用触发。

### 5.3 Rust 生成的输出 Stream（v0.3 强制）
Rust 导出的 `out_stream` 必须满足：
- `out_stream.get_next` 能迭代返回 `ArrowArray` + `ArrowSchema` 的 batch；
- 当 batch 迭代结束，返回 `ArrowArray.release == NULL` 或约定的 end-of-stream；
- `out_stream.release` 释放 stream 相关资源（包括内部持有的 Polars/Arrow buffers 的引用计数）。

### 5.4 Go 传入的输入 Stream（v0.3 强制约定）
Go 侧必须保证：
- `in_stream` 在 `bridge_plan_execute` 返回前始终有效；
- 若 `in_stream` 依赖 Go 对象（例如 byte slice），必须确保 Go GC 不会在 Rust 读取期间回收或移动关键内存。
  - 建议策略：将底层 buffer 放在 C-alloc 或使用 Go Arrow 库已提供的安全 FFI 互操作机制；
  - 兜底：在调用后 `runtime.KeepAlive(holder)`，holder 是持有所有相关 Go buffers 的对象。

### 5.5 零拷贝的“满足条件”定义（必须写入 README）
以下条件同时满足时，才声明“零拷贝”：
1. 输入 Arrow buffers 为稳定内存（不会在执行期间被移动/回收）；
2. Rust 侧对输入仅做借用，不触发 materialize 拷贝；
3. 输出 Arrow buffers 由 Rust 生成并可直接被 Go 消费（无再编码）。

否则必须在能力协商中标记为 **copy-on-boundary** 或仅声明“尽量减少拷贝”。

---

## 6. Protobuf Plan：字段号治理与版本演进策略（v0.3 强制）

### 6.1 Plan 版本字段（必须）
- `Plan.plan_version`：u32（v0.3 初始为 `1`）
- Rust 引擎必须公开：
  - `min_plan_version_supported`
  - `max_plan_version_supported`

### 6.2 字段号与 reserved 策略（强制）
为避免未来扩展冲突，采用以下规则：
1. **核心字段（稳定）使用低号段**：1–49
2. **节点 kind 扩展使用中号段**：10–199（按类别分配）
3. **表达式 kind 扩展使用中号段**：1–199（expr 内部）
4. **预留区间**：每个 message 明确 `reserved` 段，防止复用废弃字段号

示例（必须在 proto 中体现）：
- `Node.kind` 的 oneof：保留 10–49 给第一阶段算子；50–99 预留 join/window；100+ 预留 UDF 等。
- `Expr.kind`：1–49 核心表达式；50–99 预留字符串函数；100+ 预留窗口/自定义函数。

### 6.3 演进规则（强制）
- **允许**：新增字段（带默认值）、新增 oneof 分支（新算子/新表达式）
- **不允许**：更改已有字段语义；复用已废弃字段号；改变 enum 既有值含义
- 必须维护兼容：
  - Rust 支持 `max_plan_version` 与 `max_plan_version-1`（至少 N 与 N-1）
  - Go SDK 在构建 Plan 前应读取 `capabilities`，对不支持功能提前失败

---

## 7. 验收标准（Exit Criteria）与测试用例清单（v0.3 可执行）

### 7.1 Milestone 1（M1）验收
**M1 Exit Criteria**
1. 三平台（Win/Lin/Mac）可加载库并通过 `abi_version` 握手；
2. `bridge_last_error` 可用且并发下不串话；
3. 最小数据通路可执行：输入 Arrow → Rust sum → 输出标量/单列 batch；
4. Linux/macOS 通过 ASan/UBSan（至少 debug pipeline）无崩溃。

**M1 测试用例（必须实现自动化）**
- M1-T01：加载器路径优先级（ENV 绝对路径优先；APPDIR 次之）
- M1-T02：错误通路（传空指针、错误长度）返回 `ERR_INVALID_ARGUMENT` 且 last_error 非空
- M1-T03：ABI mismatch（人为改 abi_version）返回 `ERR_ABI_MISMATCH`
- M1-T04：Arrow 输入 int64 列 sum（含 null）结果正确
- M1-T05：并发执行 10 次（不同 goroutine）不崩溃且 last_error 不串话

### 7.2 Milestone 2（M2）验收
**M2 Exit Criteria**
1. Go DSL → Protobuf Plan → Rust LazyFrame → collect → Arrow 输出全链路可用；
2. 支持算子子集（至少）：memory scan、select/project、filter、with_columns、groupby-agg、sort、limit；
3. Golden tests：跨平台输出一致（同输入同 plan 结果一致）；
4. capabilities 协商：对不支持节点/表达式，Go 侧提前失败或 Rust 返回 `ERR_UNSUPPORTED`。

**M2 Golden 测试用例（必须）**
- M2-G01：select + filter（int/bool）
- M2-G02：with_columns（新增列：a+b；alias）
- M2-G03：groupby + sum/mean（含 null）
- M2-G04：sort + limit
- M2-G05：类型 cast（int→float；string→? 可延后）
- M2-G06：错误语义：引用不存在列名（ERR_PLAN_SEMANTIC）

### 7.3 Milestone 3（M3）验收
**M3 Exit Criteria**
1. CI 自动产出并发布四套动态库（win-x64, linux-x64, darwin-x64, darwin-arm64）；
2. 提供可重复运行的 benchmark suite，并产出报告（JSON/Markdown）；
3. 明确兼容矩阵：最低 glibc、macOS 签名策略、Windows runtime 选择；
4. embed 模式（若实现）提供明确限制与失败提示。

---

## 8. CI/CD 与构建兼容矩阵（v0.3 必须写清）

### 8.1 构建目标（最小交付）
| 平台 | 架构 | 产物 |
|---|---|---|
| Windows | x86_64 | `libpolars_bridge.dll` |
| Linux | x86_64 | `libpolars_bridge.so` |
| macOS | x86_64 | `libpolars_bridge.dylib` |
| macOS | arm64 | `libpolars_bridge.dylib` |

> 可选扩展：Linux arm64、Windows arm64（不在 v0.3 必须范围）。

### 8.2 Linux glibc 策略（v0.3 需定）
- 需明确“最低支持 glibc 版本”。建议：
  - 使用较老发行版 runner/container 构建（例如 Ubuntu LTS 的旧版本或 manylinux 基线）以降低 glibc 依赖版本。
- 在发布说明中写明：若用户系统 glibc 更低，可能无法加载 so。

### 8.3 macOS 签名与分发（embed 特别注意）
- 若 **不 embed**：用户以文件形式携带 dylib，依赖其本地 Gatekeeper 策略；
- 若 **embed + 解压加载**：可能触发隔离属性/签名校验失败。必须：
  - 在 README 中写明限制；
  - 或在 release pipeline 中引入 codesign/notarize（成本较高）。

### 8.4 Windows runtime 选择
- 建议优先 `windows-msvc` 目标链路，降低用户机器上 mingw 依赖风险。
- 在发布说明中写明：若依赖 MSVC runtime，是否静态链接/随包分发/由系统提供。

---

## 9. Go SDK 规范（v0.3）

### 9.1 Loader 安全要求
- 默认不走系统全局搜索路径；
- 仅允许绝对路径覆盖（ENV）；
- 加载失败时返回包含尝试路径列表的错误信息（便于排障）。

### 9.2 执行 API 的推荐形态
- `Compile(plan) -> PlanHandle`
- `Execute(plan, inputStream) -> outputStream`
- `Collect()` 作为封装（内部调用 Execute 并拉取全部 batches）

### 9.3 KeepAlive 与资源释放（强制）
- 所有跨 FFI 传递的输入 stream 相关 Go 对象必须在调用后 `runtime.KeepAlive(holder)`；
- output stream 消费完成后必须显式 close/free，触发 Rust release。

---

## 10. Protobuf v1（子集）清单与能力协商（v0.3）

### 10.1 v1 必须支持的 Node/Expr（对齐 M2 Exit）
- Node：MemoryScan, Project, Filter, WithColumns, GroupByAgg, Sort, Limit
- Expr：Col, Lit(i64,f64,bool,string), Binary(比较/四则/AND/OR), Cast(基础类型), IsNull, Alias
- DataType：Int64, Float64, Bool, Utf8（List/Struct 可延后但建议预留字段号）

### 10.2 capabilities 建议字段（JSON 或 Protobuf）
- `abi_version`
- `engine_version`
- `min_plan_version_supported`, `max_plan_version_supported`
- `supported_nodes`: ["MemoryScan","Project",...]
- `supported_exprs`: ["Col","Lit","Binary",...]
- `supported_dtypes`: ["Int64","Float64","Bool","Utf8"]
- `execution_modes`: ["collect","stream"]（v0.3 可先 collect）
- `copy_behavior`: "zero_copy_when_possible" / "copy_on_boundary"

---

## 11. 附录 A：proto 字段号预留示例（强制纳入 proto 文件）

> 说明：以下为“如何写 reserved”的规范示例，实际字段号需与你的 proto 一致。

```proto
message Node {
  uint32 id = 1;
  oneof kind {
    Scan scan = 10;
    Project project = 11;
    Filter filter = 12;
    WithColumns with_columns = 13;
    GroupByAgg groupby_agg = 14;
    Sort sort = 15;
    Limit limit = 16;

    // 预留：Join / Window / Pivot / UDF
    // reserved oneof field numbers
  }

  reserved 50 to 99;   // join/window reserved
  reserved 100 to 149; // udf reserved
}
````

---

## 12. 附录 B：最小 README 片段（发布必须包含）

* 支持平台与架构（产物列表）
* 最低 Linux glibc / macOS 版本 / Windows runtime 说明
* 如何设置 `POLARS_BRIDGE_LIB` 指向绝对路径
* 如何开启/关闭 IPC 降级（如 `POLARS_BRIDGE_DATA_MODE=arrow_c|ipc`）
* 常见错误：

  * ABI mismatch
  * Plan version unsupported
  * 加载失败（路径列表）
  * Arrow import/export 失败（提示检查生命周期与 KeepAlive）

---

## 13. v0.3 最终交付物（可检查清单）

1. `proto/`：v1 schema（含 reserved 策略）
2. `rust/`：cdylib（实现 ABI、TLS last_error、plan compile/execute、Arrow stream 输出）
3. `go/`：

   * loader（安全路径）
   * plan builder（DSL/IR → protobuf bytes）
   * executor（调用 execute；管理 KeepAlive；释放资源）
4. `tests/`：

   * smoke（M1）
   * golden（M2）
   * concurrency（last_error TLS、并发 execute）
5. `ci/`：

   * GitHub Actions workflow（多平台产物 + 发布附件）
   * 至少 Linux/macOS sanitizer pipeline
6. `bench/`：

   * 基准脚本（统一输入、统一 plan、统一输出校验）
   * 报告产物（JSON/Markdown）

