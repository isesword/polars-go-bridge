use crate::proto;
use crate::error::BridgeError;
use polars::prelude::*;

pub fn execute_plan(plan: &proto::Plan, input_json: &str) -> Result<String, BridgeError> {
    let root = plan.root.as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic("Plan has no root node".into()))?;
    
    // 解析输入 JSON 为 DataFrame
    let mut cursor = std::io::Cursor::new(input_json.as_bytes());
    let df = JsonReader::new(&mut cursor)
        .finish()
        .map_err(|e| BridgeError::Execution(format!("Failed to parse input JSON: {}", e)))?;
    
    // 执行计划
    let result_df = execute_node(root, df)?;
    
    // 将结果转换为 JSON
    let mut output = Vec::new();
    JsonWriter::new(&mut output)
        .finish(&mut result_df.clone())
        .map_err(|e| BridgeError::Execution(format!("Failed to serialize output: {}", e)))?;
    
    String::from_utf8(output)
        .map_err(|e| BridgeError::Execution(format!("Invalid UTF-8 in output: {}", e)))
}

pub fn execute_node(node: &proto::Node, input: DataFrame) -> Result<DataFrame, BridgeError> {
    use proto::node::Kind;
    
    let kind = node.kind.as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic("Node has no kind".into()))?;
    
    match kind {
        Kind::MemoryScan(scan) => {
            if scan.column_names.is_empty() {
                Ok(input)
            } else {
                input.select(&scan.column_names)
                    .map_err(|e| BridgeError::Execution(format!("MemoryScan failed: {}", e)))
            }
        }
        Kind::Project(proj) => {
            let input_child = proj.input.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Project has no input".into()))?;
            let df = execute_node(input_child, input)?;
            
            let exprs: Vec<Expr> = proj.expressions.iter()
                .map(|e| build_expr(e))
                .collect::<Result<_, _>>()?;
            
            df.lazy().select(&exprs).collect()
                .map_err(|e| BridgeError::Execution(format!("Project failed: {}", e)))
        }
        Kind::Filter(filter) => {
            let input_child = filter.input.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Filter has no input".into()))?;
            let df = execute_node(input_child, input)?;
            
            let pred = filter.predicate.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Filter has no predicate".into()))?;
            let pred_expr = build_expr(pred)?;
            
            df.lazy().filter(pred_expr).collect()
                .map_err(|e| BridgeError::Execution(format!("Filter failed: {}", e)))
        }
        Kind::WithColumns(with_cols) => {
            let input_child = with_cols.input.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("WithColumns has no input".into()))?;
            let df = execute_node(input_child, input)?;
            
            let exprs: Vec<Expr> = with_cols.expressions.iter()
                .map(|e| build_expr(e))
                .collect::<Result<_, _>>()?;
            
            df.lazy().with_columns(&exprs).collect()
                .map_err(|e| BridgeError::Execution(format!("WithColumns failed: {}", e)))
        }
        Kind::Limit(limit) => {
            let input_child = limit.input.as_ref()
                .ok_or_else(|| BridgeError::PlanSemantic("Limit has no input".into()))?;
            let df = execute_node(input_child, input)?;
            
            Ok(df.head(Some(limit.n as usize)))
        }
    }
}

fn build_expr(expr: &proto::Expr) -> Result<Expr, BridgeError> {
    use proto::expr::Kind;
    
    let kind = expr.kind.as_ref()
        .ok_or_else(|| BridgeError::PlanSemantic("Expr has no kind".into()))?;
    
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
    }
}
