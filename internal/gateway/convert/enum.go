package convert

import (
	"fmt"
	"strings"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
)

// ParseSide 解析 REST side：BUY | SELL。
func ParseSide(s string) (commonv1.Side, error) {
	switch strings.ToUpper(trim(s)) {
	case "BUY":
		return commonv1.Side_SIDE_BUY, nil
	case "SELL":
		return commonv1.Side_SIDE_SELL, nil
	default:
		return commonv1.Side_SIDE_UNSPECIFIED, fmt.Errorf("side must be BUY or SELL")
	}
}

// SideToString 将 proto Side 转为 REST 字符串。
func SideToString(s commonv1.Side) string {
	switch s {
	case commonv1.Side_SIDE_BUY:
		return "BUY"
	case commonv1.Side_SIDE_SELL:
		return "SELL"
	default:
		return ""
	}
}

// ParseOrderType 解析 REST type：LIMIT | MARKET。
func ParseOrderType(s string) (commonv1.OrderType, error) {
	switch strings.ToUpper(trim(s)) {
	case "LIMIT":
		return commonv1.OrderType_ORDER_TYPE_LIMIT, nil
	case "MARKET":
		return commonv1.OrderType_ORDER_TYPE_MARKET, nil
	default:
		return commonv1.OrderType_ORDER_TYPE_UNSPECIFIED, fmt.Errorf("type must be LIMIT or MARKET")
	}
}

// OrderTypeToString 将 proto OrderType 转为 REST 字符串。
func OrderTypeToString(t commonv1.OrderType) string {
	switch t {
	case commonv1.OrderType_ORDER_TYPE_LIMIT:
		return "LIMIT"
	case commonv1.OrderType_ORDER_TYPE_MARKET:
		return "MARKET"
	default:
		return ""
	}
}

func trim(s string) string {
	return strings.TrimSpace(s)
}
