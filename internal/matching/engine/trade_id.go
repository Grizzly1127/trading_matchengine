package engine

import (
	"encoding/binary"
	"hash/fnv"
)

// DeriveTradeID 由命令序号与买卖双方 order_id 确定性生成，便于回放与下游幂等。
func DeriveTradeID(commandSeq, makerOrderID, takerOrderID uint64) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	for _, v := range []uint64{makerOrderID, takerOrderID, commandSeq} {
		binary.BigEndian.PutUint64(buf[:], v)
		_, _ = h.Write(buf[:])
	}
	return h.Sum64()
}
