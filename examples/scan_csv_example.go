package main

import (
	"fmt"
	"log"

	"github.com/polars-go-bridge/bridge"
	"github.com/polars-go-bridge/polars"
)

func main() {
	// 加载 Polars Bridge
	brg, err := bridge.LoadBridge("./libpolars_bridge.dylib")
	if err != nil {
		log.Fatalf("Failed to load bridge: %v", err)
	}

	fmt.Println("=== Polars Go Bridge - CSV Scan Example ===")

	// 示例 1: 基本 CSV 扫描
	fmt.Println("📖 示例 1: 基本 CSV 扫描")
	fmt.Println("代码: ScanCSV(\"testdata/sample.csv\").Print()")
	lf := polars.ScanCSV("testdata/sample.csv")
	err = lf.Print(brg)
	if err != nil {
		log.Fatalf("Failed to print: %v", err)
	}

	// 示例 2: Filter - 筛选年龄大于 28 的记录
	fmt.Println("\n📖 示例 2: Filter - 筛选年龄大于 28 的记录")
	fmt.Println("代码: ScanCSV().Filter(Col(\"age\").Gt(Lit(28))).Print()")
	lf2 := polars.ScanCSV("testdata/sample.csv").
		Filter(polars.Col("age").Gt(polars.Lit(28)))
	err = lf2.Print(brg)
	if err != nil {
		log.Fatalf("Failed to print: %v", err)
	}

	// 示例 3: Select - 只选择特定列
	fmt.Println("\n📖 示例 3: Select - 只选择 name 和 age 列")
	fmt.Println("代码: ScanCSV().Select(Col(\"name\"), Col(\"age\")).Print()")
	lf3 := polars.ScanCSV("testdata/sample.csv").
		Select(polars.Col("name"), polars.Col("age"))
	err = lf3.Print(brg)
	if err != nil {
		log.Fatalf("Failed to print: %v", err)
	}

	// 示例 4: 组合操作 - Filter + Select + Limit
	fmt.Println("\n📖 示例 4: 组合操作 - Filter + Select + Limit")
	fmt.Println("代码: ScanCSV().Filter(Col(\"age\").Gt(Lit(25))).Select(Col(\"name\"), Col(\"salary\")).Limit(3).Print()")
	lf4 := polars.ScanCSV("testdata/sample.csv").
		Filter(polars.Col("age").Gt(polars.Lit(25))).
		Select(polars.Col("name"), polars.Col("salary")).
		Limit(3)
	err = lf4.Print(brg)
	if err != nil {
		log.Fatalf("Failed to print: %v", err)
	}

	// 示例 5: 复杂过滤 - 工程部门且工资大于 60000
	fmt.Println("\n📖 示例 5: 复杂过滤 - Engineering 部门且工资大于 60000")
	fmt.Println("代码: ScanCSV().Filter(Col(\"department\").Eq(Lit(\"Engineering\")).And(Col(\"salary\").Gt(Lit(60000)))).Print()")
	lf5 := polars.ScanCSV("testdata/sample.csv").
		Filter(
			polars.Col("department").Eq(polars.Lit("Engineering")).
				And(polars.Col("salary").Gt(polars.Lit(60000))),
		)
	err = lf5.Print(brg)
	if err != nil {
		log.Fatalf("Failed to print: %v", err)
	}

	fmt.Println("\n✅ 所有示例执行成功！")
}
