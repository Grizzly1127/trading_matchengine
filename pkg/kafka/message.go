package kafka

// Message 表示一条 Kafka 消息（消费或提交用）。
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
}
