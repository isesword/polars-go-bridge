package polars

import (
	"encoding/json"
	"fmt"

	"github.com/polars-go-bridge/bridge"
	pb "github.com/polars-go-bridge/proto"
	"google.golang.org/protobuf/proto"
)

// LazyFrame 惰性数据框架（延迟执行）
type LazyFrame struct {
	root   *pb.Node
	nodeID uint32
}

// NewLazyFrame 从输入数据创建 LazyFrame
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
		nodeID: 1,
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
		root:   newNode,
		nodeID: lf.nodeID,
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
		root:   newNode,
		nodeID: lf.nodeID,
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
		root:   newNode,
		nodeID: lf.nodeID,
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
		root:   newNode,
		nodeID: lf.nodeID,
	}
}

// Collect 执行查询并收集结果
func (lf *LazyFrame) Collect(brg *bridge.Bridge, inputData []map[string]interface{}) ([]map[string]interface{}, error) {
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

	// 3. 准备输入数据
	inputJSON, err := json.Marshal(inputData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal input: %w", err)
	}

	// 4. 执行查询
	outputJSON, err := brg.ExecuteSimple(handle, string(inputJSON))
	if err != nil {
		return nil, fmt.Errorf("failed to execute plan: %w", err)
	}

	// 5. 解析结果（NDJSON 格式）
	result, err := parseNDJSON(outputJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to parse output: %w", err)
	}

	return result, nil
}
