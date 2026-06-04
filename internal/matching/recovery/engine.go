package recovery

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/presence"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/symbol"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/snapshot"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
	"github.com/Grizzly1127/trading_matchengine/pkg/wal"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"
)

const defaultSnapshotEvery = 10000

// WALGroupCommitConfig WAL 组提交。
type WALGroupCommitConfig struct {
	SyncEveryRecords    int // <=1：每条 fsync；>1：批量 fsync
	SyncIntervalMs      int
	ConsumerBatchMax    int
	ConsumerBatchWaitMs int
}

// GroupCommitEnabled 是否启用组提交。
func (c WALGroupCommitConfig) GroupCommitEnabled() bool {
	return c.SyncEveryRecords > 1 || c.SyncIntervalMs > 0
}

// Config 持久化撮合引擎配置。
type Config struct {
	ShardID        string
	DataDir        string
	SnapshotEvery  uint64 // 每 N 条命令触发快照；0 表示默认 10000
	WALGroupCommit WALGroupCommitConfig
	SymbolRegistry *symbolrules.Registry
	Metrics        *metrics.Metrics
}

// WALWriterConfig 转为 pkg/wal 写配置。
func (c Config) WALWriterConfig() wal.FileWriterConfig {
	return wal.FileWriterConfig{
		SyncEveryRecords: c.WALGroupCommit.SyncEveryRecords,
		SyncIntervalMs:   c.WALGroupCommit.SyncIntervalMs,
	}
}

// Engine 持久化分片：先写 WAL 再改内存，支持快照与重启恢复。
type Engine struct {
	cfg    Config
	shard  *symbol.Shard
	wal    *wal.FileWriter
	reader *wal.FileReader

	walDir      string
	snapRoot    string
	manifest    string
	recovered   uint64
	snapshotSeq uint64
	seen        map[uint64]struct{}
	pending     []stagedItem
	mu          sync.Mutex
}

// Open 打开或创建持久化引擎并完成恢复。
func Open(cfg Config) (*Engine, error) {
	if cfg.ShardID == "" {
		return nil, fmt.Errorf("recovery: shard_id is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("recovery: data_dir is required")
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = defaultSnapshotEvery
	}
	if cfg.WALGroupCommit.SyncEveryRecords <= 0 {
		cfg.WALGroupCommit.SyncEveryRecords = 1
	}
	if cfg.WALGroupCommit.GroupCommitEnabled() {
		if cfg.WALGroupCommit.ConsumerBatchMax <= 0 {
			cfg.WALGroupCommit.ConsumerBatchMax = cfg.WALGroupCommit.SyncEveryRecords
		}
		if cfg.WALGroupCommit.ConsumerBatchWaitMs <= 0 {
			cfg.WALGroupCommit.ConsumerBatchWaitMs = 2
		}
	}

	walDir := filepath.Join(cfg.DataDir, "wal", cfg.ShardID)
	snapRoot := filepath.Join(cfg.DataDir, "snapshots", cfg.ShardID)

	w, err := wal.OpenFileWriter(walDir, cfg.WALWriterConfig())
	if err != nil {
		return nil, err
	}
	r, err := wal.OpenFileReader(walDir)
	if err != nil {
		_ = w.Close()
		return nil, err
	}

	e := &Engine{
		cfg:      cfg,
		shard:    symbol.NewShard(),
		wal:      w,
		reader:   r,
		walDir:   walDir,
		snapRoot: snapRoot,
		manifest: filepath.Join(snapRoot, "manifest.pb"),
		seen:     make(map[uint64]struct{}),
	}
	symbol.RegisterRegistry(e.shard, cfg.SymbolRegistry)
	if err := e.recover(); err != nil {
		_ = e.Close()
		return nil, err
	}
	return e, nil
}

// Shard 返回底层撮合分片（只读访问订单簿）。
func (e *Engine) Shard() *symbol.Shard {
	return e.shard
}

// LastSeq 返回当前 WAL 已写入的最大 seq。
func (e *Engine) LastSeq() uint64 {
	return e.wal.LastSeq()
}

// RecoveredOffset 返回本次启动恢复完成的 command seq。
func (e *Engine) RecoveredOffset() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.recovered
}

// MaxKafkaOffset 扫描 WAL，返回指定 partition 已持久化的最大 Kafka offset。
func (e *Engine) MaxKafkaOffset(partition uint32) (uint64, bool) {
	it, err := e.reader.ReadFrom(0)
	if err != nil {
		return 0, false
	}
	defer it.Close()

	var max uint64
	found := false
	for it.Next() {
		rec := it.Record()
		p, off, ok := kafkaOffsetFromRecord(rec)
		if !ok || p != partition {
			continue
		}
		if !found || off > max {
			max = off
			found = true
		}
	}
	if err := it.Err(); err != nil {
		return 0, false
	}
	return max, found
}

func kafkaOffsetFromRecord(rec wal.Record) (partition uint32, offset uint64, ok bool) {
	switch rec.EventType {
	case wal.EventTypeNewOrder:
		cmd := &matchingv1.NewOrderCommand{}
		if err := proto.Unmarshal(rec.Payload, cmd); err != nil {
			return 0, 0, false
		}
		if cmd.GetKafkaOffset() == 0 {
			return 0, 0, false
		}
		return cmd.GetKafkaPartition(), cmd.GetKafkaOffset(), true
	case wal.EventTypeCancelOrder:
		cmd := &matchingv1.CancelOrderCommand{}
		if err := proto.Unmarshal(rec.Payload, cmd); err != nil {
			return 0, 0, false
		}
		if cmd.GetKafkaOffset() == 0 {
			return 0, 0, false
		}
		return cmd.GetKafkaPartition(), cmd.GetKafkaOffset(), true
	default:
		return 0, 0, false
	}
}

// Close 关闭 WAL 写入器。
func (e *Engine) Close() error {
	return e.wal.Close()
}

// IsSymbolReadOnly 交易对是否因对账失败处于只读拒单。
func (e *Engine) IsSymbolReadOnly(symbol string) bool {
	if e == nil || e.shard == nil {
		return false
	}
	return e.shard.IsReadOnly(symbol)
}

// SetSymbolReadOnly 将交易对设为只读拒单。
func (e *Engine) SetSymbolReadOnly(symbol, reason string) {
	if e == nil || e.shard == nil {
		return
	}
	e.shard.SetReadOnly(symbol, reason)
}

// ActiveOrderIDs 返回 symbol 盘口活跃订单 ID。
func (e *Engine) ActiveOrderIDs(symbol string) []uint64 {
	if e == nil || e.shard == nil {
		return nil
	}
	se, ok := e.shard.Get(symbol)
	if !ok || se == nil || se.OrderBook == nil {
		return nil
	}
	active := se.OrderBook.ActiveOrders()
	out := make([]uint64, 0, len(active))
	for id := range active {
		out = append(out, id)
	}
	return out
}

// LookupOrderPresence 查询订单在撮合内存中的存在性（§5.6 对账）。
func (e *Engine) LookupOrderPresence(symbol string, orderID uint64) presence.Kind {
	if e == nil || orderID == 0 || symbol == "" {
		return presence.Unknown
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.seen[orderID]; !ok {
		return presence.Unknown
	}
	se, ok := e.shard.Get(symbol)
	if !ok || se == nil || se.OrderBook == nil {
		return presence.KnownNotInOrderbook
	}
	if se.OrderBook.HasActiveOrder(orderID) {
		return presence.InOrderbook
	}
	return presence.KnownNotInOrderbook
}

// ApplyNewOrder 先写 WAL 再撮合；重复 order_id 幂等跳过。
// 组提交模式下：单条命令 staging 后立即 CommitBatch（CLI/测试）；Kafka 路径用 ProcessBatch。
func (e *Engine) ApplyNewOrder(cmd *matchingv1.NewOrderCommand) ([]engine.Trade, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.wal.GroupCommitEnabled() {
		if err := e.stageNewOrderLocked(cmd); err != nil {
			return nil, err
		}
		out, err := e.commitBatchLocked()
		if err != nil {
			return nil, err
		}
		if len(out) == 0 {
			return nil, nil
		}
		o := out[0]
		if o.ReadOnly {
			return nil, engine.ErrSymbolReadOnly
		}
		if o.Duplicate {
			return nil, nil
		}
		return o.Trades, nil
	}

	return e.applyNewOrderImmediateLocked(cmd)
}

func (e *Engine) applyNewOrderImmediateLocked(cmd *matchingv1.NewOrderCommand) ([]engine.Trade, error) {
	if cmd == nil || cmd.GetOrder() == nil {
		return nil, fmt.Errorf("recovery: new order command is invalid")
	}

	symbolName := cmd.GetOrder().GetSymbol()
	if e.shard.IsReadOnly(symbolName) {
		return nil, engine.ErrSymbolReadOnly
	}

	orderID := cmd.GetOrder().GetOrderId()
	if orderID == 0 {
		return nil, fmt.Errorf("recovery: order_id is required")
	}
	if _, ok := e.seen[orderID]; ok {
		return nil, nil
	}

	payload, err := proto.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("recovery: marshal new order: %w", err)
	}

	walStart := time.Now()
	seq, err := e.wal.AppendNext(wal.EventTypeNewOrder, payload)
	if err != nil {
		return nil, err
	}
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.ObserveWalAppend(time.Since(walStart))
	}

	trades, err := e.applyNewOrder(cmd, seq)
	if err != nil {
		return trades, err
	}
	e.recovered = seq
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.SetWalLastSeq(seq)
	}
	if err := e.maybeSnapshot(seq); err != nil {
		return trades, err
	}
	return trades, nil
}

// ApplyCancel 先写 WAL 再撤单；订单不存在时幂等忽略。
func (e *Engine) ApplyCancel(cmd *matchingv1.CancelOrderCommand) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.wal.GroupCommitEnabled() {
		if err := e.stageCancelLocked(cmd); err != nil {
			return err
		}
		_, err := e.commitBatchLocked()
		return err
	}

	return e.applyCancelImmediateLocked(cmd)
}

func (e *Engine) applyCancelImmediateLocked(cmd *matchingv1.CancelOrderCommand) error {
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

	walStart := time.Now()
	seq, err := e.wal.AppendNext(wal.EventTypeCancelOrder, payload)
	if err != nil {
		return err
	}
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.ObserveWalAppend(time.Since(walStart))
	}

	if err := e.shard.Cancel(cmd.GetSymbol(), cmd.GetOrderId()); err != nil {
		return err
	}
	e.recovered = seq
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.SetWalLastSeq(seq)
	}
	return e.maybeSnapshot(seq)
}

func (e *Engine) recover() error {
	recoveredOffset, err := e.loadSnapshots()
	if err != nil {
		return err
	}

	if err := e.buildSeenFromWAL(); err != nil {
		return err
	}
	if err := e.replayWAL(recoveredOffset); err != nil {
		return err
	}

	e.mu.Lock()
	e.recovered = e.wal.LastSeq()
	e.mu.Unlock()
	return nil
}

func (e *Engine) loadSnapshots() (uint64, error) {
	manifest, err := snapshot.LoadManifest(e.manifest)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			manifest = nil
		} else {
			return 0, err
		}
	}

	recoveredOffset := uint64(0)
	if manifest != nil {
		recoveredOffset = manifest.GetRecoveredOffset()
	}

	symbolDirs, err := listSymbolDirs(e.snapRoot)
	if err != nil {
		return 0, err
	}

	for _, sym := range symbolDirs {
		dir := filepath.Join(e.snapRoot, sym)
		path, ok, err := snapshot.LatestPath(dir)
		if err != nil {
			return 0, err
		}
		if !ok {
			continue
		}

		snap, err := snapshot.Load(path)
		if err != nil {
			return 0, fmt.Errorf("recovery: load snapshot %q: %w", path, err)
		}
		if err := e.restoreSymbol(snap); err != nil {
			return 0, err
		}
		if snap.GetSeqId() > recoveredOffset {
			recoveredOffset = snap.GetSeqId()
		}
	}
	e.snapshotSeq = recoveredOffset
	return recoveredOffset, nil
}

func (e *Engine) restoreSymbol(snap *matchingv1.Snapshot) error {
	se := e.shard.Symbol(snap.GetSymbol())
	if err := se.OrderBook.RestoreFromSnapshot(snap); err != nil {
		return err
	}
	return se.OrderBook.ValidateWithOrderMap(snap.GetOrderMap())
}

func (e *Engine) buildSeenFromWAL() error {
	it, err := e.reader.ReadFrom(0)
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Next() {
		e.indexRecord(it.Record())
	}
	return it.Err()
}

func (e *Engine) replayWAL(fromSeq uint64) error {
	it, err := e.reader.ReadFrom(fromSeq)
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Next() {
		if err := e.applyRecord(it.Record()); err != nil {
			return err
		}
	}
	return it.Err()
}

func (e *Engine) indexRecord(rec wal.Record) {
	if rec.EventType != wal.EventTypeNewOrder {
		return
	}
	cmd := &matchingv1.NewOrderCommand{}
	if err := proto.Unmarshal(rec.Payload, cmd); err != nil {
		return
	}
	if id := cmd.GetOrder().GetOrderId(); id != 0 {
		e.seen[id] = struct{}{}
	}
}

func (e *Engine) applyRecord(rec wal.Record) error {
	if rec.SeqID <= e.snapshotSeq {
		return nil
	}
	switch rec.EventType {
	case wal.EventTypeNewOrder:
		cmd := &matchingv1.NewOrderCommand{}
		if err := proto.Unmarshal(rec.Payload, cmd); err != nil {
			return fmt.Errorf("recovery: decode new order seq %d: %w", rec.SeqID, err)
		}
		orderID := cmd.GetOrder().GetOrderId()
		if _, ok := e.seen[orderID]; ok {
			return nil
		}
		_, err := e.applyNewOrder(cmd, rec.SeqID)
		return err
	case wal.EventTypeCancelOrder:
		cmd := &matchingv1.CancelOrderCommand{}
		if err := proto.Unmarshal(rec.Payload, cmd); err != nil {
			return fmt.Errorf("recovery: decode cancel seq %d: %w", rec.SeqID, err)
		}
		return e.shard.Cancel(cmd.GetSymbol(), cmd.GetOrderId())
	default:
		return fmt.Errorf("recovery: unknown event type %d at seq %d", rec.EventType, rec.SeqID)
	}
}

func (e *Engine) applyNewOrder(cmd *matchingv1.NewOrderCommand, seq uint64) ([]engine.Trade, error) {
	order, err := engine.OrderFromProto(cmd.GetOrder())
	if err != nil {
		return nil, err
	}
	trades, err := e.shard.Match(order, seq)
	if err != nil {
		return nil, err
	}
	e.seen[order.OrderID] = struct{}{}
	return trades, nil
}

func (e *Engine) maybeSnapshot(seq uint64) error {
	if e.cfg.SnapshotEvery == 0 || seq%e.cfg.SnapshotEvery != 0 {
		return nil
	}
	return e.writeSnapshots(seq)
}

// SnapshotNow 立即对所有已注册 symbol 写快照并更新 manifest。
func (e *Engine) SnapshotNow() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.writeSnapshots(e.wal.LastSeq())
}

func (e *Engine) writeSnapshots(seq uint64) error {
	manifest := &matchingv1.ShardManifest{
		ShardId:           e.cfg.ShardID,
		RecoveredOffset:   seq,
		SymbolSeq:         make(map[string]uint64),
		UpdatedAtUnixNano: time.Now().UnixNano(),
	}

	for _, sym := range e.shard.Symbols() {
		se, ok := e.shard.Get(sym)
		if !ok {
			continue
		}
		snap := se.OrderBook.ExportSnapshot(e.cfg.ShardID, seq, time.Now())
		if err := se.OrderBook.ValidateWithOrderMap(snap.GetOrderMap()); err != nil {
			return err
		}

		dir := filepath.Join(e.snapRoot, sym)
		path := filepath.Join(dir, snapshot.Filename(seq))
		if err := snapshot.Save(path, snap); err != nil {
			return err
		}
		if err := snapshot.Prune(dir, snapshot.DefaultKeep); err != nil {
			return err
		}
		manifest.SymbolSeq[sym] = seq
	}

	return snapshot.SaveManifest(e.manifest, manifest)
}

func listSymbolDirs(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, ent := range entries {
		if ent.IsDir() {
			out = append(out, ent.Name())
		}
	}
	return out, nil
}

// NewOrderFromEngine 构造 WAL 用的 NewOrderCommand（测试/helper）。
func NewOrderFromEngine(o engine.Order, commandID uint64) *matchingv1.NewOrderCommand {
	return &matchingv1.NewOrderCommand{
		CommandId: commandID,
		Order:     engine.OrderToProto(o),
	}
}

// MustDecimal 解析测试用小数。
func MustDecimal(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}
