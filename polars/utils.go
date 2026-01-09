package polars

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"
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
