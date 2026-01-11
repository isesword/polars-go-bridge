package polars

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/polars-go-bridge/bridge"
)

// NewDataFrameFromMap 从 map 创建 DataFrame（类似 py-polars 的 DataFrame(dict) 方式）
//
// 实现思路（参考 py-polars）：
//
//  1. Go 侧只做最小化处理：提取列数据并转换为 JSON
//
//  2. 通过 FFI 立即传递给 Rust 侧
//
//  3. Rust 侧使用 Polars 的类型推断能力（Series::from_any_values）
//
//  4. 数据驻留在 Rust/Polars 端，Go 只持有句柄
//
//     data 格式: map[string]interface{}{
//     "col1": []int64{1, 2, 3},
//     "col2": []string{"a", "b", "c"},
//     "col3": []interface{}{1, nil, 3},  // 支持 nil
//     }
func NewDataFrameFromMap(brg *bridge.Bridge, data map[string]interface{}) (*DataFrame, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
	}

	// 转换为 JSON 格式
	columns, err := mapToColumnJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to convert data: %w", err)
	}

	// 序列化为 JSON
	jsonData, err := json.Marshal(columns)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// 调用 FFI 创建 DataFrame
	dfHandle, err := brg.CreateDataFrameFromColumns(jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	return newDataFrame(dfHandle, brg), nil
}

// columnData 表示单列数据
type columnData struct {
	Name   string        `json:"name"`
	Values []interface{} `json:"values"`
}

// mapToColumnJSON 将 map 转换为列数据格式
func mapToColumnJSON(data map[string]interface{}) ([]columnData, error) {
	columns := make([]columnData, 0, len(data))

	for colName, colValues := range data {
		// 获取列数据
		values, err := convertColumnValues(colValues)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", colName, err)
		}

		columns = append(columns, columnData{
			Name:   colName,
			Values: values,
		})
	}

	return columns, nil
}

// convertColumnValues 转换列数据为 []interface{}
func convertColumnValues(colValues interface{}) ([]interface{}, error) {
	v := reflect.ValueOf(colValues)

	// 如果已经是 []interface{}，直接返回
	if slice, ok := colValues.([]interface{}); ok {
		return slice, nil
	}

	// 处理其他切片类型
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("column data must be a slice, got %T", colValues)
	}

	// 转换为 []interface{}
	length := v.Len()
	result := make([]interface{}, length)

	for i := 0; i < length; i++ {
		val := v.Index(i)

		// 处理指针类型（nil 值）
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				result[i] = nil
			} else {
				result[i] = val.Elem().Interface()
			}
		} else {
			result[i] = val.Interface()
		}
	}

	return result, nil
}
