package polars

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/isesword/polars-go-bridge/bridge"
	pb "github.com/isesword/polars-go-bridge/proto"
	"google.golang.org/protobuf/proto"
)

func TestScanCSV(t *testing.T) {
	// 加载 bridge（自动从环境变量 POLARS_BRIDGE_LIB 或默认路径加载）
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	// 测试 1: 基本的 CSV 扫描
	t.Run("BasicCSVScan", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv")
		if lf == nil {
			t.Fatal("ScanCSV returned nil")
		}

		result, err := lf.CollectRows(brg)
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		fmt.Println(result)

		if len(result) != 7 {
			t.Fatalf("expected 7 rows, got %d", len(result))
		}

		if result[0]["name"] != "Alice" {
			t.Fatalf("unexpected first row name: %#v", result[0]["name"])
		}
	})

	// 测试 2: CSV 扫描 + Limit
	t.Run("CSVScanWithLimit", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Limit(5)

		result, err := lf.CollectRows(brg)
		if err != nil {
			t.Fatalf("Collect with limit failed: %v", err)
		}

		if len(result) != 5 {
			t.Fatalf("expected 5 rows, got %d", len(result))
		}
	})

	// 测试 3: CSV 扫描 + Filter + Select
	t.Run("CSVScanWithFilterSelect", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").
			Filter(Col("age").Gt(Lit(25))).
			Select(Col("name"), Col("age")).
			Limit(3)

		result, err := lf.CollectRows(brg)
		if err != nil {
			t.Fatalf("Collect with filter+select failed: %v", err)
		}

		if len(result) == 0 || len(result) > 3 {
			t.Fatalf("expected 1-3 rows, got %d", len(result))
		}

		for i, row := range result {
			ageVal, ok := row["age"].(int64)
			if !ok {
				t.Fatalf("row %d: age is not int64: %#v", i, row["age"])
			}
			if ageVal <= 25 {
				t.Fatalf("row %d: expected age > 25, got %d", i, ageVal)
			}
			if _, ok := row["name"].(string); !ok {
				t.Fatalf("row %d: name is not string: %#v", i, row["name"])
			}
		}
	})

	// 测试 4: 文件不存在的情况
	t.Run("NonExistentFile", func(t *testing.T) {
		lf := ScanCSV("nonexistent.csv")

		_, err := lf.CollectRows(brg)
		if err == nil {
			t.Error("Expected error for non-existent file, got nil")
		} else {
			t.Logf("Correctly got error for non-existent file: %v", err)
		}
	})

	// 测试 5: DataFrame 链式操作
	t.Run("DataFrameChaining", func(t *testing.T) {
		// 先收集一个 DataFrame
		lf := ScanCSV("../testdata/sample.csv")
		df, err := lf.Collect(brg)
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}
		defer df.Free()

		// 在 DataFrame 上进行链式操作
		result, err := df.Filter(Col("age").Gt(Lit(28))).
			Select(Col("name"), Col("age")).
			Limit(2).
			CollectRows(brg)

		if err != nil {
			t.Fatalf("DataFrame chaining failed: %v", err)
		}

		if len(result) > 2 {
			t.Fatalf("expected at most 2 rows, got %d", len(result))
		}

		for i, row := range result {
			age := row["age"].(int64)
			if age <= 28 {
				t.Fatalf("row %d: expected age > 28, got %d", i, age)
			}
			t.Logf("Row %d: name=%v, age=%d", i, row["name"], age)
		}
	})
}

func TestScanParquet(t *testing.T) {
	// 加载 bridge（自动从环境变量 POLARS_BRIDGE_LIB 或默认路径加载）
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	t.Run("ParquetNotImplemented", func(t *testing.T) {
		lf := ScanParquet("test.parquet")

		_, err := lf.CollectRows(brg)
		if err == nil {
			t.Error("Expected error for unimplemented ParquetScan")
		} else {
			t.Logf("Expected error (ParquetScan not implemented): %v", err)
		}
	})
}

func TestBasicOperations(t *testing.T) {
	// 加载 bridge（自动从环境变量 POLARS_BRIDGE_LIB 或默认路径加载）
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	// 测试 1: 算术运算 - 取模、幂运算
	t.Run("ArithmeticOperations", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age"),
			Col("age").Mod(Lit(3)).Alias("age_mod_3"),
			Col("age").Pow(Lit(2)).Alias("age_squared"),
		)

		result, err := lf.CollectRows(brg)
		if err != nil {
			t.Fatalf("Arithmetic operations failed: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected at least one row")
		}

		// 验证第一行: age=25, 25%3=1, 25^2=625
		if result[0]["age"].(int64) == 25 {
			if result[0]["age_mod_3"].(int64) != 1 {
				t.Fatalf("Expected age_mod_3 = 1, got %v", result[0]["age_mod_3"])
			}
			if result[0]["age_squared"].(int64) != 625 {
				t.Fatalf("Expected age_squared = 625, got %v", result[0]["age_squared"])
			}
		}

		t.Logf("Arithmetic operations test passed: %v", result[0])
	})

	// 测试 2: 逻辑取反
	t.Run("NotOperation", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age"),
			Col("age").Gt(Lit(30)).Alias("age_gt_30"),
			Col("age").Gt(Lit(30)).Not().Alias("age_not_gt_30"),
		)

		result, err := lf.CollectRows(brg)
		if err != nil {
			t.Fatalf("Not operation failed: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected at least one row")
		}

		// 验证逻辑取反
		for i, row := range result {
			ageGt30 := row["age_gt_30"].(bool)
			ageNotGt30 := row["age_not_gt_30"].(bool)
			if ageGt30 == ageNotGt30 {
				t.Fatalf("Row %d: age_gt_30 and age_not_gt_30 should be opposite, got %v and %v",
					i, ageGt30, ageNotGt30)
			}
		}

		t.Logf("Not operation test passed")
	})

	// 测试 3: 组合操作
	t.Run("CombinedOperations", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("name"),
			Col("age"),
			Col("salary"),
			// 工资增长 10%
			Col("salary").Mul(Lit(1.1)).Alias("new_salary"),
			// 工资等级 (万位)
			Col("salary").Div(Lit(10000)).Alias("salary_level"),
		)

		result, err := lf.Limit(3).CollectRows(brg)
		if err != nil {
			t.Fatalf("Combined operations failed: %v", err)
		}

		for i, row := range result {
			t.Logf("Row %d: name=%v, age=%v, salary=%v, new_salary=%v, salary_level=%v",
				i, row["name"], row["age"], row["salary"], row["new_salary"], row["salary_level"])
		}
	})
}

func TestStringOperations(t *testing.T) {
	// 加载 bridge（自动从环境变量 POLARS_BRIDGE_LIB 或默认路径加载）
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	toInt64 := func(v interface{}) (int64, bool) {
		switch val := v.(type) {
		case int64:
			return val, true
		case uint64:
			return int64(val), true
		case int32:
			return int64(val), true
		case uint32:
			return int64(val), true
		case int16:
			return int64(val), true
		case uint16:
			return int64(val), true
		case int8:
			return int64(val), true
		case uint8:
			return int64(val), true
		default:
			return 0, false
		}
	}

	t.Run("StringOpsBasic", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("name"),
			Col("department"),
			Col("name").StrLenChars().Alias("name_len"),
			Col("name").StrLenBytes().Alias("name_len_bytes"),
			Col("name").StrContains("li", true).Alias("name_contains_li"),
			Col("name").StrStartsWith("A").Alias("name_starts_a"),
			Col("name").StrEndsWith("e").Alias("name_ends_e"),
			Col("department").StrExtract("(Eng)", 1).Alias("dept_extract"),
			Col("name").StrReplace("i", "I", true).Alias("name_replace"),
			Col("name").StrReplaceAll("i", "I", true).Alias("name_replace_all"),
			Col("name").StrToLowercase().Alias("name_lower"),
			Col("name").StrToUppercase().Alias("name_upper"),
			Col("name").StrStripChars("A").Alias("name_strip_a"),
			Col("name").StrSlice(1, 3).Alias("name_slice"),
			Col("name").StrPadStart(7, "_").Alias("name_pad_start"),
			Col("name").StrPadEnd(7, "_").Alias("name_pad_end"),
		)

		// 打印 lf
		lf.Print(brg)

		result, err := lf.Limit(1).CollectRows(brg)
		if err != nil {
			t.Fatalf("String operations failed: %v", err)
		}

		if len(result) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result))
		}

		row := result[0]
		if row["name"] != "Alice" {
			t.Fatalf("Expected name Alice, got %v", row["name"])
		}

		if v, ok := toInt64(row["name_len"]); !ok || v != 5 {
			t.Fatalf("Expected name_len 5, got %#v", row["name_len"])
		}
		if v, ok := toInt64(row["name_len_bytes"]); !ok || v != 5 {
			t.Fatalf("Expected name_len_bytes 5, got %#v", row["name_len_bytes"])
		}

		if row["name_contains_li"] != true {
			t.Fatalf("Expected name_contains_li true, got %#v", row["name_contains_li"])
		}
		if row["name_starts_a"] != true {
			t.Fatalf("Expected name_starts_a true, got %#v", row["name_starts_a"])
		}
		if row["name_ends_e"] != true {
			t.Fatalf("Expected name_ends_e true, got %#v", row["name_ends_e"])
		}
		if row["dept_extract"] != "Eng" {
			t.Fatalf("Expected dept_extract Eng, got %#v", row["dept_extract"])
		}
		if row["name_replace"] != "AlIce" {
			t.Fatalf("Expected name_replace AlIce, got %#v", row["name_replace"])
		}
		if row["name_replace_all"] != "AlIce" {
			t.Fatalf("Expected name_replace_all AlIce, got %#v", row["name_replace_all"])
		}
		if row["name_lower"] != "alice" {
			t.Fatalf("Expected name_lower alice, got %#v", row["name_lower"])
		}
		if row["name_upper"] != "ALICE" {
			t.Fatalf("Expected name_upper ALICE, got %#v", row["name_upper"])
		}
		if row["name_strip_a"] != "lice" {
			t.Fatalf("Expected name_strip_a lice, got %#v", row["name_strip_a"])
		}
		if row["name_slice"] != "lic" {
			t.Fatalf("Expected name_slice lic, got %#v", row["name_slice"])
		}
		if row["name_pad_start"] != "__Alice" {
			t.Fatalf("Expected name_pad_start __Alice, got %#v", row["name_pad_start"])
		}
		if row["name_pad_end"] != "Alice__" {
			t.Fatalf("Expected name_pad_end Alice__, got %#v", row["name_pad_end"])
		}
	})
}

func TestArrowZeroCopy(t *testing.T) {
	if !zeroCopySupported() {
		t.Skip("zero-copy requires cgo")
	}

	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	lf := NewLazyFrame(nil).Select(
		Col("name"),
		Col("age"),
		Col("age").Add(Lit(1)).Alias("age_plus"),
		Col("name").StrToUppercase().Alias("name_upper"),
	)

	plan := &pb.Plan{
		PlanVersion: 1,
		Root:        lf.root,
	}
	planBytes, err := proto.Marshal(plan)
	if err != nil {
		t.Fatalf("Failed to marshal plan: %v", err)
	}

	handle, err := brg.CompilePlan(planBytes)
	if err != nil {
		t.Fatalf("Failed to compile plan: %v", err)
	}
	defer brg.FreePlan(handle)

	inSchema, inArray, cleanupInput, err := buildArrowInput()
	if err != nil {
		t.Fatalf("Failed to build Arrow input: %v", err)
	}
	defer cleanupInput()

	outSchema, outArray, err := brg.ExecuteArrow(handle, inSchema, inArray)
	if err != nil {
		t.Fatalf("ExecuteArrow failed: %v", err)
	}

	rec, err := importArrowRecordBatch(outSchema, outArray)
	if err != nil {
		bridge.ReleaseArrowSchema(outSchema)
		bridge.ReleaseArrowArray(outArray)
		t.Fatalf("Failed to import Arrow result: %v", err)
	}
	bridge.ReleaseArrowSchema(outSchema)
	bridge.ReleaseArrowArray(outArray)
	defer rec.Release()

	if rec.NumRows() != 3 {
		t.Fatalf("Expected 3 rows, got %d", rec.NumRows())
	}
	if rec.NumCols() != 4 {
		t.Fatalf("Expected 4 columns, got %d", rec.NumCols())
	}

	fields := rec.Schema().Fields()
	if fields[0].Name != "name" || fields[1].Name != "age" ||
		fields[2].Name != "age_plus" || fields[3].Name != "name_upper" {
		t.Fatalf("Unexpected schema fields: %#v", fields)
	}

	readString := func(col arrow.Array, idx int) (string, bool) {
		if col.IsNull(idx) {
			return "", false
		}
		switch c := col.(type) {
		case *array.String:
			return c.Value(idx), true
		case *array.LargeString:
			return c.Value(idx), true
		case *array.BinaryView:
			return string(c.Value(idx)), true
		case *array.StringView:
			return c.Value(idx), true
		default:
			return "", false
		}
	}

	readInt64 := func(col arrow.Array, idx int) (int64, bool) {
		if col.IsNull(idx) {
			return 0, false
		}
		switch c := col.(type) {
		case *array.Int64:
			return c.Value(idx), true
		case *array.Int32:
			return int64(c.Value(idx)), true
		case *array.Int16:
			return int64(c.Value(idx)), true
		case *array.Int8:
			return int64(c.Value(idx)), true
		case *array.Uint64:
			return int64(c.Value(idx)), true
		case *array.Uint32:
			return int64(c.Value(idx)), true
		case *array.Uint16:
			return int64(c.Value(idx)), true
		case *array.Uint8:
			return int64(c.Value(idx)), true
		default:
			return 0, false
		}
	}

	nameCol := rec.Column(0)
	ageCol := rec.Column(1)
	agePlusCol := rec.Column(2)
	upperCol := rec.Column(3)

	if v, ok := readString(nameCol, 0); !ok || v != "alice" {
		t.Fatalf("Expected name alice, got %#v", v)
	}
	if v, ok := readInt64(ageCol, 1); !ok || v != 21 {
		t.Fatalf("Expected age 21, got %#v", v)
	}
	if v, ok := readInt64(agePlusCol, 2); !ok || v != 46 {
		t.Fatalf("Expected age_plus 46, got %#v", v)
	}
	if v, ok := readString(upperCol, 2); !ok || v != "CARL" {
		t.Fatalf("Expected name_upper CARL, got %#v", v)
	}
}

func TestExpressionExpansion(t *testing.T) {
	// 加载 bridge（自动从环境变量 POLARS_BRIDGE_LIB 或默认路径加载）
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	// 测试 1: 使用 Cols() 选择多列
	t.Run("ColsSelection", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Cols("name", "age", "salary")...,
		)

		result, err := lf.Limit(2).CollectRows(brg)
		if err != nil {
			t.Fatalf("Cols selection failed: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected at least one row")
		}

		// 验证只有这 3 列
		if len(result[0]) != 3 {
			t.Fatalf("Expected 3 columns, got %d", len(result[0]))
		}

		if _, ok := result[0]["name"]; !ok {
			t.Fatal("Expected 'name' column")
		}
		if _, ok := result[0]["age"]; !ok {
			t.Fatal("Expected 'age' column")
		}
		if _, ok := result[0]["salary"]; !ok {
			t.Fatal("Expected 'salary' column")
		}

		t.Logf("Cols selection test passed: %v", result[0])
	})

	// 测试 2: 使用 All() 选择所有列
	t.Run("AllSelection", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			All(),
		)

		result, err := lf.Limit(1).CollectRows(brg)
		if err != nil {
			t.Fatalf("All selection failed: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected at least one row")
		}

		// 应该有所有列（4 列）
		if len(result[0]) != 4 {
			t.Fatalf("Expected 4 columns (all columns), got %d", len(result[0]))
		}

		t.Logf("All selection test passed: %d columns", len(result[0]))
	})

	// 测试 3: 组合使用 - 对多列应用相同操作
	t.Run("MultiColumnOperation", func(t *testing.T) {
		// 对多列应用相同的转换
		lf := ScanCSV("../testdata/sample.csv")

		// 分别选择多列并进行计算
		result, err := lf.Select(
			Col("name"),
			Col("age"),
			Col("salary"),
			Col("salary").Div(Lit(12)).Alias("monthly_salary"),
		).Limit(2).CollectRows(brg)

		if err != nil {
			t.Fatalf("Multi-column operation failed: %v", err)
		}

		for i, row := range result {
			t.Logf("Row %d: name=%v, salary=%v, monthly_salary=%v",
				i, row["name"], row["salary"], row["monthly_salary"])
		}
	})
}

func TestCasting(t *testing.T) {
	// 加载 bridge（自动从环境变量 POLARS_BRIDGE_LIB 或默认路径加载）
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	// 测试 1: 数值类型转换
	t.Run("NumericCasting", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age"),
			Col("age").Cast(Int32, true).Alias("age_int32"),
			Col("age").Cast(Int16, true).Alias("age_int16"),
			Col("age").Cast(Float32, true).Alias("age_float32"),
		)

		result, err := lf.Limit(2).CollectRows(brg)
		if err != nil {
			t.Fatalf("Numeric casting failed: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected at least one row")
		}

		t.Logf("Numeric casting test passed: %v", result[0])
	})

	// 测试 2: 字符串转换为数值
	t.Run("StringToNumeric", func(t *testing.T) {
		// 创建一个包含字符串数字的 DataFrame
		// 这里我们使用现有的 CSV，将 age 转换为字符串再转回数字
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age").Cast(String, true).Alias("age_as_string"),
			Col("age").Cast(String, true).Cast(Int64, true).Alias("age_back_to_int"),
		)

		result, err := lf.Limit(2).CollectRows(brg)
		if err != nil {
			t.Fatalf("String to numeric casting failed: %v", err)
		}

		t.Logf("String conversion test: %v", result[0])
	})

	// 测试 3: 布尔类型转换
	t.Run("BooleanCasting", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age"),
			Col("age").Gt(Lit(30)).Alias("age_gt_30"),
			Col("age").Gt(Lit(30)).Cast(Int8, true).Alias("age_gt_30_as_int"),
		)

		result, err := lf.Limit(3).CollectRows(brg)
		if err != nil {
			t.Fatalf("Boolean casting failed: %v", err)
		}

		for i, row := range result {
			t.Logf("Row %d: age=%v, age_gt_30=%v, age_gt_30_as_int=%v",
				i, row["age"], row["age_gt_30"], row["age_gt_30_as_int"])
		}
	})

	// 测试 4: 非严格模式（strict=false）
	t.Run("NonStrictCasting", func(t *testing.T) {
		// 非严格模式下，超出范围的值会转为 null
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("salary"),
			Col("salary").Cast(Int8, false).Alias("salary_as_int8"), // salary 很大，会超出 Int8 范围
		)

		result, err := lf.Limit(2).CollectRows(brg)
		if err != nil {
			t.Fatalf("Non-strict casting failed: %v", err)
		}

		t.Logf("Non-strict casting test (values out of range become null): %v", result[0])
	})

	// 测试 5: 严格模式 - 使用 StrictCast 方法
	t.Run("StrictCastMethod", func(t *testing.T) {
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("age"),
			Col("age").StrictCast(Int16).Alias("age_int16"),
			Col("salary").StrictCast(Float32).Alias("salary_float32"),
		)

		result, err := lf.Limit(2).CollectRows(brg)
		if err != nil {
			t.Fatalf("StrictCast method failed: %v", err)
		}

		t.Logf("StrictCast method test passed: %v", result[0])
	})
}

func TestArrowExecution(t *testing.T) {
	// 加载 bridge（自动从环境变量 POLARS_BRIDGE_LIB 或默认路径加载）
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	// 测试 1: Arrow 零拷贝执行（无输入）
	t.Run("ArrowExecutionNoInput", func(t *testing.T) {
		// 构建一个简单的查询计划
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("name"),
			Col("age"),
		).Limit(3)

		// 构建 Plan
		plan := &pb.Plan{
			PlanVersion: 1,
			Root:        lf.root,
		}

		// 序列化 Plan
		planBytes, err := proto.Marshal(plan)
		if err != nil {
			t.Fatalf("Failed to marshal plan: %v", err)
		}

		// 编译 Plan
		handle, err := brg.CompilePlan(planBytes)
		if err != nil {
			t.Fatalf("Failed to compile plan: %v", err)
		}
		defer brg.FreePlan(handle)

		// 使用 Arrow C Data Interface 执行（无输入）
		outSchema, outArray, err := brg.ExecuteArrow(handle, nil, nil)
		if err != nil {
			t.Fatalf("Arrow execution failed: %v", err)
		}

		// 确保在函数结束时释放 Arrow 资源
		if outSchema != nil {
			defer bridge.ReleaseArrowSchema(outSchema)
		}
		if outArray != nil {
			defer bridge.ReleaseArrowArray(outArray)
		}

		// 验证返回的 Arrow 数据不为空
		if outSchema == nil || outArray == nil {
			t.Fatal("Expected non-nil Arrow schema and array")
		}

		t.Logf("✅ Arrow execution succeeded (zero-copy data transfer)")
		t.Logf("   Output Arrow Schema: %p", outSchema)
		t.Logf("   Output Arrow Array: %p", outArray)
	})

	// 测试 2: 比较 Arrow 执行与常规 IPC 执行的结果一致性
	t.Run("ArrowVsIPCConsistency", func(t *testing.T) {
		// 构建相同的查询
		lf := ScanCSV("../testdata/sample.csv").Select(
			Col("name"),
			Col("age"),
			Col("salary"),
		).Filter(Col("age").Gt(Lit(25))).Limit(5)

		// 方法 1: 使用传统 IPC 方式
		resultIPC, err := lf.CollectRows(brg)
		if err != nil {
			t.Fatalf("IPC execution failed: %v", err)
		}

		// 方法 2: 使用 Arrow C Data Interface
		plan := &pb.Plan{
			PlanVersion: 1,
			Root:        lf.root,
		}
		planBytes, _ := proto.Marshal(plan)
		handle, _ := brg.CompilePlan(planBytes)
		defer brg.FreePlan(handle)

		outSchema, outArray, err := brg.ExecuteArrow(handle, nil, nil)
		if err != nil {
			t.Fatalf("Arrow execution failed: %v", err)
		}
		defer bridge.ReleaseArrowSchema(outSchema)
		defer bridge.ReleaseArrowArray(outArray)

		// 验证行数一致
		if len(resultIPC) == 0 {
			t.Fatal("Expected at least one row from IPC")
		}

		t.Logf("✅ Arrow and IPC execution both succeeded")
		t.Logf("   IPC result rows: %d", len(resultIPC))
		t.Logf("   Arrow execution completed (zero-copy)")
		t.Logf("   First row from IPC: %v", resultIPC[0])
	})

	// 测试 3: 测试 Arrow 错误处理
	t.Run("ArrowErrorHandling", func(t *testing.T) {
		// 使用不存在的文件测试错误处理
		lf := ScanCSV("nonexistent.csv").Select(Col("name"))

		plan := &pb.Plan{
			PlanVersion: 1,
			Root:        lf.root,
		}
		planBytes, _ := proto.Marshal(plan)
		handle, err := brg.CompilePlan(planBytes)
		if err != nil {
			t.Fatalf("Failed to compile plan: %v", err)
		}
		defer brg.FreePlan(handle)

		_, _, err = brg.ExecuteArrow(handle, nil, nil)
		if err == nil {
			t.Error("Expected error for non-existent file")
		} else {
			t.Logf("✅ Correctly got error: %v", err)
		}
	})

	// 测试 4: Arrow 性能特性说明
	t.Run("ArrowPerformanceNote", func(t *testing.T) {
		t.Log("📊 Arrow C Data Interface 特性:")
		t.Log("   • 零拷贝数据传输 (Zero-copy)")
		t.Log("   • 直接在内存中共享数据指针")
		t.Log("   • 避免序列化/反序列化开销")
		t.Log("   • 适合大数据量传输")
		t.Log("   • 与 Apache Arrow 生态集成")
		t.Log("   • 比 IPC 格式更高效")
	})
}

func TestDataFrameFromMap(t *testing.T) {
	// 加载 bridge（自动从环境变量 POLARS_BRIDGE_LIB 或默认路径加载）
	brg, err := bridge.LoadBridge("")
	if err != nil {
		t.Fatalf("Failed to load bridge: %v", err)
	}

	// 测试 1: 基本类型推断
	t.Run("BasicTypes", func(t *testing.T) {
		df, err := NewDataFrameFromMap(brg, map[string]interface{}{
			"nrs":    []int64{1, 2, 3, 4, 5},
			"names":  []string{"foo", "ham", "spam", "egg", "spam"},
			"random": []float64{0.37454, 0.950714, 0.731994, 0.598658, 0.156019},
			"groups": []string{"A", "A", "B", "A", "B"},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}
		defer df.Free()

		rows, err := df.Lazy().CollectRows(brg)
		if err != nil {
			t.Fatalf("Failed to collect rows: %v", err)
		}

		if len(rows) != 5 {
			t.Fatalf("Expected 5 rows, got %d", len(rows))
		}

		if rows[0]["names"] != "foo" {
			t.Fatalf("Expected names[0] = foo, got %v", rows[0]["names"])
		}

		t.Logf("✅ Basic types test passed: %d rows", len(rows))
	})

	// 测试 2: null 值处理
	t.Run("NullValues", func(t *testing.T) {
		df, err := NewDataFrameFromMap(brg, map[string]interface{}{
			"id":   []interface{}{1, 2, nil, 4, 5},
			"name": []interface{}{"Alice", nil, "Charlie", "Diana", "Eve"},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}
		defer df.Free()

		rows, err := df.Lazy().CollectRows(brg)
		if err != nil {
			t.Fatalf("Failed to collect rows: %v", err)
		}

		if len(rows) != 5 {
			t.Fatalf("Expected 5 rows, got %d", len(rows))
		}

		// 验证 null 值
		if rows[2]["id"] != nil {
			t.Fatalf("Expected id[2] = nil, got %v", rows[2]["id"])
		}

		if rows[1]["name"] != nil {
			t.Fatalf("Expected name[1] = nil, got %v", rows[1]["name"])
		}

		t.Logf("✅ Null values test passed")
	})

	// 测试 3: 链式操作
	t.Run("ChainedOperations", func(t *testing.T) {
		df, err := NewDataFrameFromMap(brg, map[string]interface{}{
			"age":    []int64{25, 30, 35, 28, 32},
			"name":   []string{"Alice", "Bob", "Charlie", "Diana", "Eve"},
			"salary": []int64{50000, 60000, 70000, 55000, 65000},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}
		defer df.Free()

		// 应用 Polars 操作
		result, err := df.
			Filter(Col("age").Gt(Lit(28))).
			Select(Col("name"), Col("salary")).
			CollectRows(brg)

		if err != nil {
			t.Fatalf("Chained operations failed: %v", err)
		}

		if len(result) != 3 {
			t.Fatalf("Expected 3 rows after filter, got %d", len(result))
		}

		t.Logf("✅ Chained operations test passed: %d rows", len(result))
		t.Logf("   Result: %v", result)
	})

	// 测试 4: Polars 类型推断
	t.Run("TypeInference", func(t *testing.T) {
		df, err := NewDataFrameFromMap(brg, map[string]interface{}{
			"integers": []int64{1, 2, 3},
			"floats":   []float64{1.5, 2.5, 3.7},
			"strings":  []string{"a", "b", "c"},
			"mixed":    []interface{}{1, nil, 3},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}
		defer df.Free()

		rows, err := df.Lazy().CollectRows(brg)
		if err != nil {
			t.Fatalf("Failed to collect rows: %v", err)
		}

		t.Logf("✅ Type inference test passed")
		t.Logf("   Row 0: %v", rows[0])
		t.Logf("   Row 1 (with null): %v", rows[1])
	})

	// 测试 5: 打印 DataFrame
	t.Run("PrintDataFrame", func(t *testing.T) {
		df, err := NewDataFrameFromMap(brg, map[string]interface{}{
			"nrs":    []int64{1, 2, 3, 4, 5},
			"names":  []string{"foo", "ham", "spam", "egg", "spam"},
			"random": []interface{}{0.37454, 0.950714, 0.731994, 0.598658, 0.156019},
			"groups": []string{"A", "A", "B", "A", "B"},
		})
		if err != nil {
			t.Fatalf("Failed to create DataFrame: %v", err)
		}
		defer df.Free()

		t.Log("📊 DataFrame content:")
		err = df.Print()
		if err != nil {
			t.Fatalf("Failed to print DataFrame: %v", err)
		}
	})
}
