package recovery

import (
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/wal"
	"google.golang.org/protobuf/proto"
)

type stagedKind byte

const (
	stagedNewOrder stagedKind = 1
	stagedCancel   stagedKind = 2
)

type stagedItem struct {
	kind      stagedKind
	newOrder  *matchingv1.NewOrderCommand
	cancel    *matchingv1.CancelOrderCommand
	seq       uint64
	duplicate bool
	readOnly  bool
}

// CommandOutcome 单条命令在批次提交后的结果（供 consumer 发布）。
type CommandOutcome struct {
	Trades    []engine.Trade
	WalSeq    uint64
	Duplicate bool
	ReadOnly  bool
}

// GroupCommitEnabled 是否启用 WAL 组提交（需配合 consumer 批量或单条 CommitBatch）。
func (e *Engine) GroupCommitEnabled() bool {
	return e != nil && e.wal != nil && e.wal.GroupCommitEnabled()
}

func (e *Engine) stageNewOrderLocked(cmd *matchingv1.NewOrderCommand) error {
	if cmd == nil || cmd.GetOrder() == nil {
		return fmt.Errorf("recovery: new order command is invalid")
	}
	symbolName := cmd.GetOrder().GetSymbol()
	if e.shard.IsReadOnly(symbolName) {
		e.pending = append(e.pending, stagedItem{kind: stagedNewOrder, newOrder: cmd, readOnly: true})
		return nil
	}
	orderID := cmd.GetOrder().GetOrderId()
	if orderID == 0 {
		return fmt.Errorf("recovery: order_id is required")
	}
	if _, ok := e.seen[orderID]; ok {
		e.pending = append(e.pending, stagedItem{kind: stagedNewOrder, newOrder: cmd, duplicate: true})
		return nil
	}
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("recovery: marshal new order: %w", err)
	}
	seq, err := e.wal.AppendNext(wal.EventTypeNewOrder, payload)
	if err != nil {
		return err
	}
	e.pending = append(e.pending, stagedItem{kind: stagedNewOrder, newOrder: cmd, seq: seq})
	return nil
}

func (e *Engine) stageCancelLocked(cmd *matchingv1.CancelOrderCommand) error {
	if cmd == nil {
		return fmt.Errorf("recovery: cancel command is nil")
	}
	if cmd.GetSymbol() == "" || cmd.GetOrderId() == 0 {
		return fmt.Errorf("recovery: symbol and order_id are required")
	}
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("recovery: marshal cancel: %w", err)
	}
	seq, err := e.wal.AppendNext(wal.EventTypeCancelOrder, payload)
	if err != nil {
		return err
	}
	e.pending = append(e.pending, stagedItem{kind: stagedCancel, cancel: cmd, seq: seq})
	return nil
}

// StageNewOrder 仅追加 WAL（组提交批次内）；须随后 CommitBatch。
func (e *Engine) StageNewOrder(cmd *matchingv1.NewOrderCommand) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stageNewOrderLocked(cmd)
}

// StageCancel 仅追加 WAL（组提交批次内）；须随后 CommitBatch。
func (e *Engine) StageCancel(cmd *matchingv1.CancelOrderCommand) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stageCancelLocked(cmd)
}

// CommitBatch 对当前 pending 执行一次 WAL Sync，再按序 apply（先 WAL durable 再内存）。
func (e *Engine) CommitBatch() ([]CommandOutcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.commitBatchLocked()
}

func (e *Engine) commitBatchLocked() ([]CommandOutcome, error) {
	n := len(e.pending)
	if n == 0 {
		return nil, nil
	}
	out := make([]CommandOutcome, n)

	if e.wal.GroupCommitEnabled() {
		walStart := time.Now()
		if err := e.wal.Sync(); err != nil {
			return nil, err
		}
		if e.cfg.Metrics != nil {
			e.cfg.Metrics.ObserveWalGroupCommit(time.Since(walStart), n)
		}
	}

	for i, item := range e.pending {
		switch item.kind {
		case stagedNewOrder:
			if item.readOnly {
				out[i] = CommandOutcome{ReadOnly: true}
				continue
			}
			if item.duplicate {
				out[i] = CommandOutcome{Duplicate: true}
				continue
			}
			trades, err := e.applyNewOrder(item.newOrder, item.seq)
			if err != nil {
				return nil, err
			}
			e.recovered = item.seq
			if e.cfg.Metrics != nil {
				e.cfg.Metrics.SetWalLastSeq(item.seq)
			}
			if err := e.maybeSnapshot(item.seq); err != nil {
				return nil, err
			}
			out[i] = CommandOutcome{Trades: trades, WalSeq: item.seq}
		case stagedCancel:
			if err := e.shard.Cancel(item.cancel.GetSymbol(), item.cancel.GetOrderId()); err != nil {
				return nil, err
			}
			e.recovered = item.seq
			if e.cfg.Metrics != nil {
				e.cfg.Metrics.SetWalLastSeq(item.seq)
			}
			if err := e.maybeSnapshot(item.seq); err != nil {
				return nil, err
			}
			out[i] = CommandOutcome{WalSeq: item.seq}
		default:
			return nil, fmt.Errorf("recovery: unknown staged kind %d", item.kind)
		}
	}
	e.pending = e.pending[:0]
	return out, nil
}
