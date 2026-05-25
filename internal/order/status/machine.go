package status

import "fmt"

// 订单状态常量（与 orders.status 列一致）。
const (
	Pending   = "PENDING"
	Accepted  = "ACCEPTED"
	Partial   = "PARTIAL"
	Canceling = "CANCELING"
	Filled    = "FILLED"
	Canceled  = "CANCELED"
	Rejected  = "REJECTED"
)

// MatchEventType 与 matching.v1.MatchEventType 数值对齐。
type MatchEventType int32

const (
	MatchUnspecified    MatchEventType = 0
	MatchOrderAccepted  MatchEventType = 1
	MatchOrderFilled    MatchEventType = 2
	MatchOrderPartial   MatchEventType = 3
	MatchOrderCanceled  MatchEventType = 4
)

// TargetStatus 将 MatchEvent 映射为目标订单状态。
func TargetStatus(ev MatchEventType) (string, error) {
	switch ev {
	case MatchOrderAccepted:
		return Accepted, nil
	case MatchOrderPartial:
		return Partial, nil
	case MatchOrderFilled:
		return Filled, nil
	case MatchOrderCanceled:
		return Canceled, nil
	default:
		return "", fmt.Errorf("unsupported match event type %d", ev)
	}
}

// IsTerminal 是否为终态。
func IsTerminal(s string) bool {
	switch s {
	case Filled, Canceled, Rejected:
		return true
	default:
		return false
	}
}

// IsProgressFromMatching 表示撮合进展类目标状态（接单/部成/全成）。
func IsProgressFromMatching(target string) bool {
	switch target {
	case Accepted, Partial, Filled:
		return true
	default:
		return false
	}
}

// FillWinsOverCancel 撤单中间态 CANCELING 下，撮合进展事件优先于撤单意图。
func FillWinsOverCancel(from, target string) bool {
	return from == Canceling && IsProgressFromMatching(target)
}

// AllowedFromStatuses 返回允许迁移到 target 的源状态列表。
func AllowedFromStatuses(target string) []string {
	switch target {
	case Accepted:
		return []string{Pending, Canceling}
	case Partial:
		return []string{Pending, Accepted, Partial, Canceling}
	case Filled:
		return []string{Pending, Accepted, Partial, Canceling}
	case Canceling:
		return []string{Pending, Accepted, Partial}
	case Canceled:
		return []string{Pending, Accepted, Partial, Canceling}
	default:
		return nil
	}
}

// CanTransition 判断 from→to 是否合法。
func CanTransition(from, to string) bool {
	if from == to {
		return true
	}
	for _, s := range AllowedFromStatuses(to) {
		if s == from {
			return true
		}
	}
	return false
}
