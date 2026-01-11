use crate::proto;
use crate::error::BridgeError;
use crate::expr_str;
use polars::prelude::*;
use polars::prelude::PlPath;
use polars::prelude::IntoLazy;

/// 执行 Plan，返回结果的 Arrow IPC 格式字节流
pub fn execute_plan(plan: &proto::Plan) -> Result<Vec<u8>, BridgeError> {
    let result_df = execute_plan_df(plan, None)?;
    df_to_ipc(&result_df)
}

/// 执行 Plan 并打印结果（调用 Polars 原生的 Display）
pub fn execute_and_print(plan: &proto::Plan) -> Result<(), BridgeError> {
    let result_df = execute_plan_df(plan, None)?;
    df_print(&result_df)
}

/// 执行 Plan，返回 DataFrame（可复用）
pub fn execute_plan_df(
    plan: &proto::Plan,
    input_df: Option<&DataFrame>,
) -> Result<DataFrame, BridgeError> {
    let root = plan.root.as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic("Plan has no root node".into()))?;

    // 从 Plan 构建 LazyFrame（根据节点类型自动决定数据源）
    let lf = build_lazy_frame(root, input_df)?;

    // 执行 LazyFrame
    let result_df = lf.collect()
        .map_err(|e| BridgeError::Execution(format!("Failed to collect LazyFrame: {}", e)))?;

    Ok(result_df)
}

/// 将 DataFrame 转换为 Arrow IPC 格式
pub fn df_to_ipc(df: &DataFrame) -> Result<Vec<u8>, BridgeError> {
    let mut output = Vec::new();
    let mut df = df.clone();
    IpcWriter::new(&mut output)
        .finish(&mut df)
        .map_err(|e| BridgeError::Execution(format!("Failed to serialize to IPC: {}", e)))?;
    Ok(output)
}

/// 打印 DataFrame（使用 Polars 原生 Display）
pub fn df_print(df: &DataFrame) -> Result<(), BridgeError> {
    println!("{}\n", df);
    Ok(())
}

/// 从 Node 构建 LazyFrame（递归）
fn build_lazy_frame(
    node: &proto::Node,
    input_df: Option<&DataFrame>,
) -> Result<LazyFrame, BridgeError> {
    use proto::node::Kind;
    
    let kind = node.kind.as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic("Node has no kind".into()))?;
    
    match kind {
        Kind::CsvScan(scan) => {
            // 从 CSV 文件路径懒加载
            LazyCsvReader::new(PlPath::new(scan.path.as_str()))
                .finish()
                .map_err(|e| BridgeError::Execution(format!("CsvScan failed for '{}': {}", scan.path, e)))
        }
        Kind::ParquetScan(_scan) => {
            // TODO: Parquet 支持
            Err(BridgeError::Unsupported("ParquetScan not yet implemented".into()))
        }
        Kind::MemoryScan(scan) => {
            let df = input_df.ok_or_else(|| {
                BridgeError::Unsupported("MemoryScan requires input DataFrame".into())
            })?;
            let mut lf = df.clone().lazy();
            if !scan.column_names.is_empty() {
                let exprs: Vec<Expr> = scan
                    .column_names
                    .iter()
                    .map(|name| col(name.as_str()))
                    .collect();
                lf = lf.select(&exprs);
            }
            Ok(lf)
        }
        Kind::Project(proj) => {
            let input_node = proj.input.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Project has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_df)?;
            
            let exprs: Vec<Expr> = proj.expressions.iter()
                .map(|e| build_expr(e))
                .collect::<Result<_, _>>()?;
            
            Ok(lf.select(&exprs))
        }
        Kind::Filter(filter) => {
            let input_node = filter.input.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Filter has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_df)?;
            
            let pred = filter.predicate.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Filter has no predicate".into()))?;
            let pred_expr = build_expr(pred)?;
            
            Ok(lf.filter(pred_expr))
        }
        Kind::WithColumns(with_cols) => {
            let input_node = with_cols.input.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("WithColumns has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_df)?;
            
            let exprs: Vec<Expr> = with_cols.expressions.iter()
                .map(|e| build_expr(e))
                .collect::<Result<_, _>>()?;
            
            Ok(lf.with_columns(&exprs))
        }
        Kind::Limit(limit) => {
            let input_node = limit.input.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Limit has no input".into()))?;
            let lf = build_lazy_frame(input_node, input_df)?;
            
            Ok(lf.limit(limit.n as u32))
        }
    }
}

pub fn build_expr(expr: &proto::Expr) -> Result<Expr, BridgeError> {
    use proto::expr::Kind;
    
    let kind = expr.kind.as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic("Expr has no kind".into()))?;

    if let Some(result) = expr_str::build_string_expr(kind) {
        return result;
    }
    
    match kind {
        Kind::Col(col) => {
            Ok(polars::prelude::col(&col.name))
        }
        Kind::Lit(lit) => {
            use proto::literal::Value;
            let val = lit.value.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Literal has no value".into()))?;
            
            match val {
                Value::IntVal(v) => Ok(polars::prelude::lit(*v)),
                Value::FloatVal(v) => Ok(polars::prelude::lit(*v)),
                Value::BoolVal(v) => Ok(polars::prelude::lit(*v)),
                Value::StringVal(v) => Ok(polars::prelude::lit(v.as_str())),
                Value::NullVal(_) => Ok(polars::prelude::lit(NULL)),
            }
        }
        Kind::Binary(bin) => {
            let left = bin.left.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Binary has no left".into()))?;
            let right = bin.right.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Binary has no right".into()))?;
            
            let left_expr = build_expr(left)?;
            let right_expr = build_expr(right)?;
            
            use proto::BinaryOperator;
            match proto::BinaryOperator::try_from(bin.op) {
                Ok(BinaryOperator::Add) => Ok(left_expr + right_expr),
                Ok(BinaryOperator::Sub) => Ok(left_expr - right_expr),
                Ok(BinaryOperator::Mul) => Ok(left_expr * right_expr),
                Ok(BinaryOperator::Div) => Ok(left_expr / right_expr),
                Ok(BinaryOperator::Eq) => Ok(left_expr.eq(right_expr)),
                Ok(BinaryOperator::Ne) => Ok(left_expr.neq(right_expr)),
                Ok(BinaryOperator::Lt) => Ok(left_expr.lt(right_expr)),
                Ok(BinaryOperator::Le) => Ok(left_expr.lt_eq(right_expr)),
                Ok(BinaryOperator::Gt) => Ok(left_expr.gt(right_expr)),
                Ok(BinaryOperator::Ge) => Ok(left_expr.gt_eq(right_expr)),
                Ok(BinaryOperator::And) => Ok(left_expr.and(right_expr)),
                Ok(BinaryOperator::Or) => Ok(left_expr.or(right_expr)),
                Ok(BinaryOperator::Mod) => Ok(left_expr % right_expr),
                Ok(BinaryOperator::Pow) => Ok(left_expr.pow(right_expr)),
                Ok(BinaryOperator::Xor) => Ok(left_expr.xor(right_expr)),
                Err(_) => Err(BridgeError::Unsupported(format!("Unknown binary operator: {}", bin.op))),
            }
        }
        Kind::Alias(alias) => {
            let expr = alias.expr.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Alias has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.alias(&alias.name))
        }
        Kind::IsNull(is_null) => {
            let expr = is_null.expr.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("IsNull has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.is_null())
        }
        Kind::Not(not) => {
            let expr = not.expr.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Not has no expr".into()))?;
            let e = build_expr(expr)?;
            Ok(e.not())
        }
        Kind::Wildcard(_) => {
            // Wildcard 表示选择所有列
            Ok(polars::prelude::col("*"))
        }
        Kind::Exclude(_) => {
            // Exclude 功能暂未实现
            Err(BridgeError::Unsupported("Exclude operation is not yet supported".into()))
        }
        Kind::Cast(cast) => {
            // 处理类型转换
            let expr = cast.expr.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Cast has no expr".into()))?;
            let e = build_expr(expr)?;
            
            // 将 proto DataType 转换为 Polars DataType
            let target_type = match proto::DataType::try_from(cast.data_type) {
                Ok(proto::DataType::Int64) => DataType::Int64,
                Ok(proto::DataType::Int32) => DataType::Int32,
                Ok(proto::DataType::Int16) => DataType::Int16,
                Ok(proto::DataType::Int8) => DataType::Int8,
                Ok(proto::DataType::Uint64) => DataType::UInt64,
                Ok(proto::DataType::Uint32) => DataType::UInt32,
                Ok(proto::DataType::Uint16) => DataType::UInt16,
                Ok(proto::DataType::Uint8) => DataType::UInt8,
                Ok(proto::DataType::Float64) => DataType::Float64,
                Ok(proto::DataType::Float32) => DataType::Float32,
                Ok(proto::DataType::Bool) => DataType::Boolean,
                Ok(proto::DataType::Utf8) => DataType::String,
                Ok(proto::DataType::Date) => DataType::Date,
                Ok(proto::DataType::Datetime) => DataType::Datetime(TimeUnit::Microseconds, None),
                Ok(proto::DataType::Time) => DataType::Time,
                Err(_) => return Err(BridgeError::Unsupported(
                    format!("Unknown data type: {}", cast.data_type)
                )),
            };
            
            // 根据 strict 参数选择 cast 或 strict_cast
            if cast.strict {
                Ok(e.strict_cast(target_type))
            } else {
                Ok(e.cast(target_type))
            }
        }
        _ => Err(BridgeError::Unsupported(
            "Expression type is not yet supported".into(),
        )),
    }
}
