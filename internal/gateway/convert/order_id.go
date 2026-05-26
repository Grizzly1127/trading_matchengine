package convert

import (
	"fmt"
	"strconv"
)

// OrderIDToString 将 uint64 订单号转为十进制字符串（REST JSON）。
func OrderIDToString(id uint64) string {
	return strconv.FormatUint(id, 10)
}

// ParseOrderID 解析路径/查询参数中的 order_id。
func ParseOrderID(s string) (uint64, error) {
	s = trim(s)
	if s == "" {
		return 0, fmt.Errorf("order_id is required")
	}
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("order_id must be a decimal uint64")
	}
	return id, nil
}
