package polars

import (
	"fmt"

	"github.com/polars-go-bridge/bridge"
	pb "github.com/polars-go-bridge/proto"
	"google.golang.org/protobuf/proto"
)

// LazyFrame 惰性数据框架（延迟执行）
type LazyFrame struct {
	root    *pb.Node
	nodeID  uint32
	inputDF *DataFrame
}

// NewLazyFrame 从内存数据创建 LazyFrame
func NewLazyFrame(data []map[string]interface{}) *LazyFrame {
	return &LazyFrame{
		root: &pb.Node{
			Id: 1,
			Kind: &pb.Node_MemoryScan{
				MemoryScan: &pb.MemoryScan{
					ColumnNames: []string{}, // 空表示所有列
				},
			},
		},
		nodeID:  1,
		inputDF: nil,
	}
}

// ScanCSV 从 CSV 文件路径创建 LazyFrame（懒加载）
func ScanCSV(path string) *LazyFrame {
	return &LazyFrame{
		root: &pb.Node{
			Id: 1,
			Kind: &pb.Node_CsvScan{
				CsvScan: &pb.CsvScan{
					Path: path,
				},
			},
		},
		nodeID:  1,
		inputDF: nil,
	}
}

// ScanParquet 从 Parquet 文件路径创建 LazyFrame（懒加载）
func ScanParquet(path string) *LazyFrame {
	return &LazyFrame{
		root: &pb.Node{
			Id: 1,
			Kind: &pb.Node_ParquetScan{
				ParquetScan: &pb.ParquetScan{
					Path: path,
				},
			},
		},
		nodeID:  1,
		inputDF: nil,
	}
}

// nextNodeID 获取下一个节点 ID
func (lf *LazyFrame) nextNodeID() uint32 {
	lf.nodeID++
	return lf.nodeID
}

// Filter 过滤行
func (lf *LazyFrame) Filter(predicate Expr) *LazyFrame {
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Filter{
			Filter: &pb.Filter{
				Input:     lf.root,
				Predicate: predicate.toProto(),
			},
		},
	}

	return &LazyFrame{
		root:    newNode,
		nodeID:  lf.nodeID,
		inputDF: lf.inputDF,
	}
}

// Select 选择列
func (lf *LazyFrame) Select(exprs ...Expr) *LazyFrame {
	protoExprs := make([]*pb.Expr, len(exprs))
	for i, expr := range exprs {
		protoExprs[i] = expr.toProto()
	}

	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Project{
			Project: &pb.Project{
				Input:       lf.root,
				Expressions: protoExprs,
			},
		},
	}

	return &LazyFrame{
		root:    newNode,
		nodeID:  lf.nodeID,
		inputDF: lf.inputDF,
	}
}

// WithColumns 添加或修改列
func (lf *LazyFrame) WithColumns(exprs ...Expr) *LazyFrame {
	protoExprs := make([]*pb.Expr, len(exprs))
	for i, expr := range exprs {
		protoExprs[i] = expr.toProto()
	}

	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_WithColumns{
			WithColumns: &pb.WithColumns{
				Input:       lf.root,
				Expressions: protoExprs,
			},
		},
	}

	return &LazyFrame{
		root:    newNode,
		nodeID:  lf.nodeID,
		inputDF: lf.inputDF,
	}
}

// Limit 限制行数
func (lf *LazyFrame) Limit(n uint64) *LazyFrame {
	newNode := &pb.Node{
		Id: lf.nextNodeID(),
		Kind: &pb.Node_Limit{
			Limit: &pb.Limit{
				Input: lf.root,
				N:     n,
			},
		},
	}

	return &LazyFrame{
		root:    newNode,
		nodeID:  lf.nodeID,
		inputDF: lf.inputDF,
	}
}

// Collect 执行查询并返回 DataFrame（使用完请调用 Free）
func (lf *LazyFrame) Collect(brg *bridge.Bridge) (*DataFrame, error) {
	if lf == nil {
		return nil, fmt.Errorf("lazyframe is nil")
	}
	if lf.inputDF != nil && lf.inputDF.brg != brg {
		return nil, fmt.Errorf("bridge mismatch for input dataframe")
	}
	// 1. 构建 Plan
	plan := &pb.Plan{
		PlanVersion: 1,
		Root:        lf.root,
	}

	// 2. 编译 Plan
	planBytes, err := proto.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal plan: %w", err)
	}

	handle, err := brg.CompilePlan(planBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to compile plan: %w", err)
	}
	defer brg.FreePlan(handle)

	// 3. 执行查询并返回 DataFrame 句柄
	inputHandle := uint64(0)
	if lf.inputDF != nil {
		inputHandle = lf.inputDF.handle
	}
	dfHandle, err := brg.CollectPlanDF(handle, inputHandle)
	if err != nil {
		return nil, fmt.Errorf("failed to collect dataframe: %w", err)
	}

	return newDataFrame(dfHandle, brg), nil
}

// CollectRows 执行查询并返回行数据（用于兼容旧接口）
func (lf *LazyFrame) CollectRows(brg *bridge.Bridge) ([]map[string]interface{}, error) {
	df, err := lf.Collect(brg)
	if err != nil {
		return nil, err
	}
	defer df.Free()

	rows, err := df.Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to export rows: %w", err)
	}
	return rows, nil
}

// Print 执行查询并直接打印结果（使用 Polars 原生的 Display）
func (lf *LazyFrame) Print(brg *bridge.Bridge) error {
	if lf == nil {
		return fmt.Errorf("lazyframe is nil")
	}
	if lf.inputDF != nil {
		df, err := lf.Collect(brg)
		if err != nil {
			return err
		}
		defer df.Free()
		return df.Print()
	}

	// 1. 构建 Plan
	plan := &pb.Plan{
		PlanVersion: 1,
		Root:        lf.root,
	}

	// 2. 编译 Plan
	planBytes, err := proto.Marshal(plan)
	if err != nil {
		return fmt.Errorf("failed to marshal plan: %w", err)
	}

	handle, err := brg.CompilePlan(planBytes)
	if err != nil {
		return fmt.Errorf("failed to compile plan: %w", err)
	}
	defer brg.FreePlan(handle)

	// 3. 执行并打印（调用 Polars 原生的 Display）
	err = brg.ExecuteAndPrint(handle)
	if err != nil {
		return fmt.Errorf("failed to execute and print: %w", err)
	}

	return nil
}
