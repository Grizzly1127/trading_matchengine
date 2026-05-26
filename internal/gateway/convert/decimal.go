package convert

import (
	"fmt"
	"strings"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
)

// DecimalFromString 将 REST 十进制字符串转为 proto Decimal。
func DecimalFromString(s string) (*commonv1.Decimal, error) {
	s = trim(s)
	if s == "" {
		return nil, fmt.Errorf("decimal value is required")
	}
	return &commonv1.Decimal{Value: s}, nil
}

// DecimalToString 将 proto Decimal 转为 REST 字符串；nil 返回空串。
func DecimalToString(d *commonv1.Decimal) string {
	if d == nil {
		return ""
	}
	return strings.TrimSpace(d.GetValue())
}
