package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/polars-go-bridge/bridge"
	"github.com/polars-go-bridge/polars"
	pb "github.com/polars-go-bridge/proto"
	"google.golang.org/protobuf/proto"
)

func main() {
	// 加载动态库
	brg, err := bridge.LoadBridge("")
	if err != nil {
		log.Fatalf("Failed to load bridge: %v", err)
	}

	// 获取版本信息
	abiVer := brg.AbiVersion()
	fmt.Printf("ABI Version: %d\n", abiVer)

	engineVer, err := brg.EngineVersion()
	if err != nil {
		log.Fatalf("Failed to get engine version: %v", err)
	}
	fmt.Printf("Engine Version: %s\n", engineVer)

	caps, err := brg.Capabilities()
	if err != nil {
		log.Fatalf("Failed to get capabilities: %v", err)
	}
	fmt.Printf("Capabilities:\n%s\n", caps)

	// fmt.Println("\n=== Testing CSV Processing ===")

	// // 测试 CSV 读取
	// if err := testCSVProcessing(brg); err != nil {
	// 	log.Fatalf("CSV processing test failed: %v", err)
	// }

	// fmt.Println("\n=== Testing Fluent API ===")

	// 测试 Fluent API
	if err := testFluentAPI(brg); err != nil {
		log.Fatalf("Fluent API test failed: %v", err)
	}

	fmt.Println("\n✅ All tests passed!")
}

func testCSVProcessing(brg *bridge.Bridge) error {
	// 1. 读取 CSV 文件
	fmt.Println("\n1. Reading CSV file...")
	csvData, err := readCSVToJSON("testdata/large_sample.csv")
	if err != nil {
		return fmt.Errorf("failed to read CSV: %w", err)
	}
	fmt.Printf("   Loaded %d records\n", len(csvData))

	// 2. 创建查询计划：读取所有列，限制前 5 行
	fmt.Println("\n2. Creating query plan (MemoryScan + Limit 5)...")
	plan := &pb.Plan{
		PlanVersion: 1,
		Root: &pb.Node{
			Id: 1,
			Kind: &pb.Node_Limit{
				Limit: &pb.Limit{
					N: 5,
					Input: &pb.Node{
						Id: 2,
						Kind: &pb.Node_MemoryScan{
							MemoryScan: &pb.MemoryScan{
								ColumnNames: []string{}, // 空表示所有列
							},
						},
					},
				},
			},
		},
	}

	// 3. 编译计划
	fmt.Println("\n3. Compiling plan...")
	planBytes, err := proto.Marshal(plan)
	if err != nil {
		return fmt.Errorf("failed to marshal plan: %w", err)
	}

	handle, err := brg.CompilePlan(planBytes)
	if err != nil {
		return fmt.Errorf("failed to compile plan: %w", err)
	}
	defer brg.FreePlan(handle)
	fmt.Printf("   Plan compiled, handle: %d\n", handle)

	// 4. 执行计划
	fmt.Println("\n4. Executing plan...")
	inputJSON, err := json.Marshal(csvData)
	if err != nil {
		return fmt.Errorf("failed to marshal input: %w", err)
	}

	outputJSON, err := brg.ExecuteSimple(handle, string(inputJSON))
	if err != nil {
		return fmt.Errorf("failed to execute plan: %w", err)
	}

	// 5. 显示结果
	fmt.Println("\n5. Results (first 5 rows):")

	// Polars 输出 NDJSON 格式（每行一个 JSON 对象）
	result, err := parseNDJSON(outputJSON)
	if err != nil {
		return fmt.Errorf("failed to parse output: %w", err)
	}

	for i, row := range result {
		fmt.Printf("   Row %d: %v\n", i+1, row)
	}

	return nil
}

// parseNDJSON 解析 NDJSON 格式（每行一个 JSON 对象）
func parseNDJSON(ndjson string) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	scanner := bufio.NewScanner(strings.NewReader(ndjson))

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var row map[string]interface{}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("failed to parse line: %w", err)
		}
		result = append(result, row)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func readCSVToJSON(filepath string) ([]map[string]interface{}, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 2 {
		return []map[string]interface{}{}, nil
	}

	headers := records[0]
	var result []map[string]interface{}

	for _, record := range records[1:] {
		if len(record) != len(headers) {
			continue
		}
		row := make(map[string]interface{})
		for i, header := range headers {
			row[header] = record[i]
		}
		result = append(result, row)
	}

	return result, nil
}

func testFluentAPI(brg *bridge.Bridge) error {
	// 1. 读取 CSV 数据
	fmt.Println("\n1. Reading CSV file...")
	csvData, err := readCSVToJSON("testdata/large_sample.csv")
	if err != nil {
		return fmt.Errorf("failed to read CSV: %w", err)
	}
	fmt.Printf("   Loaded %d records\n", len(csvData))

	// 2. 使用 Fluent API 构建查询
	fmt.Println("\n2. Building query with Fluent API...")
	fmt.Println("   Query: Filter department=='Engineering' -> Select name,salary -> Limit 3")

	df := polars.NewLazyFrame(csvData)
	result, err := df.
		Filter(polars.Col("department").Eq(polars.Lit("Engineering"))).
		Select(polars.Col("name"), polars.Col("salary"), polars.Col("years_experience")).
		Limit(3).
		Collect(brg, csvData)

	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// 3. 显示结果
	fmt.Println("\n3. Results:")
	for i, row := range result {
		fmt.Printf("   %d. Name: %v, Salary: %v, Experience: %v years\n",
			i+1, row["name"], row["salary"], row["years_experience"])
	}

	// 4. 测试复杂表达式
	fmt.Println("\n4. Testing complex expression...")
	fmt.Println("   Query: Filter salary > 70000 AND years_experience >= 5")

	result2, err := polars.NewLazyFrame(csvData).
		Filter(
			polars.Col("salary").Gt(polars.Lit("70000")).
				And(polars.Col("years_experience").Ge(polars.Lit("5"))),
		).
		Limit(5).
		Collect(brg, csvData)

	if err != nil {
		return fmt.Errorf("failed to execute complex query: %w", err)
	}

	fmt.Printf("   Found %d records\n", len(result2))
	for i, row := range result2 {
		fmt.Printf("   %d. %v (%v dept) - $%v, %v years\n",
			i+1, row["name"], row["department"], row["salary"], row["years_experience"])
	}

	return nil
}
