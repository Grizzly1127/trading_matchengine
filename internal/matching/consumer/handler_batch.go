package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"google.golang.org/protobuf/proto"
)

type stagedMessage struct {
	msg kafka.Message
	env *matchingv1.OrderCommandEnvelope
}

// ProcessBatch 组提交路径：先批量写 WAL，再一次 Sync+apply，再聚合发布。
// processing 指标与单条 Process 一致：摊销后的 (stage+sync+apply) + 本条分摊的 Publish 墙钟。
func (h *Handler) ProcessBatch(ctx context.Context, msgs []kafka.Message) error {
	if h.Engine == nil || !h.Engine.GroupCommitEnabled() {
		return fmt.Errorf("consumer: ProcessBatch requires WAL group commit")
	}
	if len(msgs) == 0 {
		return nil
	}

	batchStart := time.Now()
	start := msgs[0].Offset
	staged := make([]*stagedMessage, 0, len(msgs))
	for _, msg := range msgs {
		sc, err := h.stageDecodedMessage(msg)
		if err != nil {
			return fmt.Errorf("consumer: stage offset %d: %w", msg.Offset, err)
		}
		staged = append(staged, sc)
	}

	outcomes, err := h.Engine.CommitBatch()
	if err != nil {
		return fmt.Errorf("consumer: commit batch from offset %d: %w", start, err)
	}
	if len(outcomes) != len(msgs) {
		return fmt.Errorf("consumer: outcomes %d != msgs %d", len(outcomes), len(msgs))
	}

	prepDur := time.Since(batchStart)
	n := time.Duration(len(msgs))
	perCmdPrep := prepDur / n

	outs, err := h.buildOutboundBatch(staged, outcomes)
	if err != nil {
		return err
	}

	pubStart := time.Now()
	if len(outs) > 0 {
		if err := h.Publisher.PublishBatch(ctx, outs); err != nil {
			if h.Metrics != nil {
				h.Metrics.ObservePublishError()
			}
			return err
		}
	}
	perCmdPub := time.Since(pubStart) / n

	for _, msg := range msgs {
		if h.Metrics != nil {
			cmdWall := perCmdPrep + perCmdPub
			h.Metrics.ObserveProcessing(cmdWall)
			h.Metrics.SetLastProcessedOffset(uint64(msg.Offset))
		}
	}
	return nil
}

func (h *Handler) stageDecodedMessage(msg kafka.Message) (*stagedMessage, error) {
	env := &matchingv1.OrderCommandEnvelope{}
	if err := proto.Unmarshal(msg.Value, env); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	if no := env.GetNewOrder(); no != nil {
		no.KafkaPartition = uint32(msg.Partition)
		no.KafkaOffset = uint64(msg.Offset)
		if err := h.Engine.StageNewOrder(no); err != nil {
			return nil, err
		}
		return &stagedMessage{msg: msg, env: env}, nil
	}
	if co := env.GetCancelOrder(); co != nil {
		co.KafkaPartition = uint32(msg.Partition)
		co.KafkaOffset = uint64(msg.Offset)
		if err := h.Engine.StageCancel(co); err != nil {
			return nil, err
		}
		return &stagedMessage{msg: msg, env: env}, nil
	}
	return nil, fmt.Errorf("empty envelope at offset %d", msg.Offset)
}

func (h *Handler) buildOutboundBatch(staged []*stagedMessage, outcomes []recovery.CommandOutcome) ([]publisher.Outbound, error) {
	outs := make([]publisher.Outbound, 0, len(staged))
	for i, sc := range staged {
		o := outcomes[i]
		if o.ReadOnly || o.Duplicate {
			continue
		}
		if no := sc.env.GetNewOrder(); no != nil {
			if h.Metrics != nil {
				h.Metrics.ObserveTrades(len(o.Trades))
			}
			outs = append(outs, publisher.BuildNewOrderEvents(h.Engine.Shard(), no, o.Trades, o.WalSeq, o.Duplicate))
			continue
		}
		if co := sc.env.GetCancelOrder(); co != nil {
			outs = append(outs, publisher.BuildCancelEvents(co, o.WalSeq))
			continue
		}
		return nil, fmt.Errorf("consumer: empty envelope at offset %d", sc.msg.Offset)
	}
	return outs, nil
}
