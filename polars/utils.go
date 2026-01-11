package polars

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

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

// parseArrowIPC 解析 Arrow IPC 文件格式二进制为行数据
func parseArrowIPC(ipcBytes []byte) ([]map[string]interface{}, error) {
	reader, err := ipc.NewMappedFileReader(ipcBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create IPC file reader: %w", err)
	}
	defer reader.Close()

	var rows []map[string]interface{}
	schema := reader.Schema()
	fields := schema.Fields()

	nRecs := reader.NumRecords()
	for batchIdx := 0; batchIdx < nRecs; batchIdx++ {
		rec, err := reader.RecordBatch(batchIdx)
		if err != nil {
			return nil, fmt.Errorf("failed to read record batch %d: %w", batchIdx, err)
		}

		nRows := int(rec.NumRows())
		nCols := int(rec.NumCols())

		for i := 0; i < nRows; i++ {
			row := make(map[string]interface{}, nCols)
			for colIdx := 0; colIdx < nCols; colIdx++ {
				field := fields[colIdx]
				col := rec.Column(colIdx)

				if col.IsNull(i) {
					row[field.Name] = nil
					continue
				}

				switch c := col.(type) {
				case *array.Int64:
					row[field.Name] = c.Value(i)
				case *array.Int32:
					row[field.Name] = int64(c.Value(i))
				case *array.Int16:
					row[field.Name] = int64(c.Value(i))
				case *array.Int8:
					row[field.Name] = int64(c.Value(i))
				case *array.Uint64:
					row[field.Name] = c.Value(i)
				case *array.Uint32:
					row[field.Name] = uint64(c.Value(i))
				case *array.Uint16:
					row[field.Name] = uint64(c.Value(i))
				case *array.Uint8:
					row[field.Name] = uint64(c.Value(i))
				case *array.Float64:
					row[field.Name] = c.Value(i)
				case *array.Float32:
					row[field.Name] = float64(c.Value(i))
				case *array.Boolean:
					row[field.Name] = c.Value(i)
				case *array.String:
					row[field.Name] = c.Value(i)
				case *array.LargeString:
					row[field.Name] = c.Value(i)
				case *array.BinaryView:
					// StringView in Polars is represented as BinaryView in Arrow
					row[field.Name] = string(c.Value(i))
				case *array.StringView:
					// Polars StringView type (optimized string representation)
					row[field.Name] = c.Value(i)
				default:
					return nil, fmt.Errorf("unsupported Arrow type %T for field %s", col, field.Name)
				}
			}
			rows = append(rows, row)
		}

		rec.Release()
	}

	return rows, nil
}
