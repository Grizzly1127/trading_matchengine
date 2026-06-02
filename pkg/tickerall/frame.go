package tickerall

import "encoding/json"

// IsWSFrame 判断 payload 是否为 §8.2 ticker@all 推送帧（snapshot/delta/heartbeat）。
func IsWSFrame(payload []byte) bool {
	_, ok := ParseFrame(payload)
	return ok
}

// ParseFrame 解析并校验 ticker@all WS 帧。
func ParseFrame(payload []byte) (*Frame, bool) {
	if len(payload) == 0 {
		return nil, false
	}
	var f Frame
	if err := json.Unmarshal(payload, &f); err != nil {
		return nil, false
	}
	switch f.Type {
	case TypeSnapshot, TypeDelta, TypeHeartbeat:
	default:
		return nil, false
	}
	if f.Stream == "" || f.SnapshotID == "" {
		return nil, false
	}
	return &f, true
}
