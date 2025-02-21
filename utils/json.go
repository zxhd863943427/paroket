package utils

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type JSONMap map[string]interface{}

// 实现 sql.Scanner 接口
func (jm *JSONMap) Scan(value interface{}) error {
	if value == nil {
		*jm = nil
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to unmarshal JSON: value is not a byte slice")
	}
	return json.Unmarshal(bytes, jm)
}

// 实现 driver.Valuer 接口
func (jm JSONMap) Value() (driver.Value, error) {
	if jm == nil {
		return nil, nil
	}
	v, err := json.Marshal(jm)
	return v, err
}
