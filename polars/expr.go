package polars

import (
	pb "github.com/isesword/polars-go-bridge/proto"
)

// 数据类型常量
var (
	Int64    = pb.DataType_INT64
	Int32    = pb.DataType_INT32
	Int16    = pb.DataType_INT16
	Int8     = pb.DataType_INT8
	UInt64   = pb.DataType_UINT64
	UInt32   = pb.DataType_UINT32
	UInt16   = pb.DataType_UINT16
	UInt8    = pb.DataType_UINT8
	Float64  = pb.DataType_FLOAT64
	Float32  = pb.DataType_FLOAT32
	Boolean  = pb.DataType_BOOL
	String   = pb.DataType_UTF8
	Date     = pb.DataType_DATE
	Datetime = pb.DataType_DATETIME
	Time     = pb.DataType_TIME
)

// Expr 表达式构建器
type Expr struct {
	inner *pb.Expr
}

// Col 创建列引用表达式
func Col(name string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Col{
				Col: &pb.Column{
					Name: name,
				},
			},
		},
	}
}

// Cols 创建多列引用表达式（表达式展开）
func Cols(names ...string) []Expr {
	exprs := make([]Expr, len(names))
	for i, name := range names {
		exprs[i] = Col(name)
	}
	return exprs
}

// All 选择所有列（表达式展开）
func All() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Wildcard{
				Wildcard: &pb.Wildcard{},
			},
		},
	}
}

// Lit 创建字面量表达式
func Lit(value interface{}) Expr {
	var lit *pb.Literal

	switch v := value.(type) {
	case int:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: int64(v)},
		}
	case int64:
		lit = &pb.Literal{
			Value: &pb.Literal_IntVal{IntVal: v},
		}
	case float64:
		lit = &pb.Literal{
			Value: &pb.Literal_FloatVal{FloatVal: v},
		}
	case float32:
		lit = &pb.Literal{
			Value: &pb.Literal_FloatVal{FloatVal: float64(v)},
		}
	case bool:
		lit = &pb.Literal{
			Value: &pb.Literal_BoolVal{BoolVal: v},
		}
	case string:
		lit = &pb.Literal{
			Value: &pb.Literal_StringVal{StringVal: v},
		}
	case nil:
		lit = &pb.Literal{
			Value: &pb.Literal_NullVal{NullVal: &pb.NullValue{}},
		}
	default:
		// 默认转换为字符串
		lit = &pb.Literal{
			Value: &pb.Literal_StringVal{StringVal: ""},
		}
	}

	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Lit{
				Lit: lit,
			},
		},
	}
}

// 二元操作符辅助函数
func (e Expr) binaryOp(op pb.BinaryOperator, other Expr) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Binary{
				Binary: &pb.BinaryExpr{
					Left:  e.inner,
					Op:    op,
					Right: other.inner,
				},
			},
		},
	}
}

// Eq 等于
func (e Expr) Eq(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_EQ, other)
}

// Ne 不等于
func (e Expr) Ne(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_NE, other)
}

// Lt 小于
func (e Expr) Lt(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_LT, other)
}

// Le 小于等于
func (e Expr) Le(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_LE, other)
}

// Gt 大于
func (e Expr) Gt(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_GT, other)
}

// Ge 大于等于
func (e Expr) Ge(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_GE, other)
}

// Add 加法
func (e Expr) Add(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_ADD, other)
}

// Sub 减法
func (e Expr) Sub(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_SUB, other)
}

// Mul 乘法
func (e Expr) Mul(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_MUL, other)
}

// Div 除法
func (e Expr) Div(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_DIV, other)
}

// And 逻辑与
func (e Expr) And(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_AND, other)
}

// Or 逻辑或
func (e Expr) Or(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_OR, other)
}

// Mod 取模运算 (%)
func (e Expr) Mod(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_MOD, other)
}

// Pow 幂运算 (**)
func (e Expr) Pow(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_POW, other)
}

// Xor 异或运算 (^)
func (e Expr) Xor(other Expr) Expr {
	return e.binaryOp(pb.BinaryOperator_XOR, other)
}

// Alias 设置别名
func (e Expr) Alias(name string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Alias{
				Alias: &pb.Alias{
					Expr: e.inner,
					Name: name,
				},
			},
		},
	}
}

// IsNull 检查是否为空
func (e Expr) IsNull() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_IsNull{
				IsNull: &pb.IsNull{
					Expr: e.inner,
				},
			},
		},
	}
}

// Not 逻辑取反 (~)
func (e Expr) Not() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Not{
				Not: &pb.Not{
					Expr: e.inner,
				},
			},
		},
	}
}

// Cast 类型转换（严格模式）
// 示例: Col("age").Cast(Int32, true)
func (e Expr) Cast(dataType pb.DataType, strict bool) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_Cast{
				Cast: &pb.Cast{
					Expr:     e.inner,
					DataType: dataType,
					Strict:   strict,
				},
			},
		},
	}
}

// StrictCast 严格模式类型转换（转换失败报错）
// 示例: Col("age").StrictCast(Int32)
func (e Expr) StrictCast(dataType pb.DataType) Expr {
	return e.Cast(dataType, true)
}

// toProto 转换为 Protobuf 表达式
func (e Expr) toProto() *pb.Expr {
	return e.inner
}
