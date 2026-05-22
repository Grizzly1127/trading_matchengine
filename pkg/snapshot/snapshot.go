package snapshot

import (
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"google.golang.org/protobuf/proto"
)

const (
	// DefaultKeep 默认保留的快照份数（架构 §5.3）。
	DefaultKeep = 3
	snapshotPrefix = "snapshot_"
	snapshotSuffix = ".pb"
)

var (
	ErrNilSnapshot  = errors.New("snapshot: snapshot is nil")
	ErrNilManifest  = errors.New("snapshot: manifest is nil")
	ErrChecksum     = errors.New("snapshot: checksum mismatch")
	ErrEmptyPath    = errors.New("snapshot: path is required")
)

// Save 将快照 protobuf 原子写入 path（.tmp → fsync → rename），并自动填充 checksum。
func Save(path string, snap *matchingv1.Snapshot) error {
	if path == "" {
		return ErrEmptyPath
	}
	if snap == nil {
		return ErrNilSnapshot
	}

	toSave := proto.Clone(snap).(*matchingv1.Snapshot)
	toSave.Checksum = ComputeChecksum(toSave)

	data, err := proto.Marshal(toSave)
	if err != nil {
		return fmt.Errorf("snapshot: marshal: %w", err)
	}
	return writeAtomic(path, data)
}

// Load 从 path 加载快照并校验 checksum。
func Load(path string) (*matchingv1.Snapshot, error) {
	if path == "" {
		return nil, ErrEmptyPath
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("snapshot: read %q: %w", path, err)
	}

	snap := &matchingv1.Snapshot{}
	if err := proto.Unmarshal(data, snap); err != nil {
		return nil, fmt.Errorf("snapshot: unmarshal: %w", err)
	}
	if err := VerifyChecksum(snap); err != nil {
		return nil, err
	}
	return snap, nil
}

// SaveManifest 将 shard manifest 原子写入 path。
func SaveManifest(path string, manifest *matchingv1.ShardManifest) error {
	if path == "" {
		return ErrEmptyPath
	}
	if manifest == nil {
		return ErrNilManifest
	}

	data, err := proto.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("snapshot: marshal manifest: %w", err)
	}
	return writeAtomic(path, data)
}

// LoadManifest 从 path 加载 shard manifest。
func LoadManifest(path string) (*matchingv1.ShardManifest, error) {
	if path == "" {
		return nil, ErrEmptyPath
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("snapshot: read manifest %q: %w", path, err)
	}

	manifest := &matchingv1.ShardManifest{}
	if err := proto.Unmarshal(data, manifest); err != nil {
		return nil, fmt.Errorf("snapshot: unmarshal manifest: %w", err)
	}
	return manifest, nil
}

// ComputeChecksum 计算 bids + asks 的 FNV-64a 校验和（对齐架构 §5.3）。
func ComputeChecksum(snap *matchingv1.Snapshot) uint64 {
	if snap == nil {
		return 0
	}
	h := fnv.New64a()
	for _, level := range snap.GetBids() {
		hashPriceLevel(h, level)
	}
	for _, level := range snap.GetAsks() {
		hashPriceLevel(h, level)
	}
	return h.Sum64()
}

// VerifyChecksum 校验快照 checksum 是否与 bids/asks 一致。
func VerifyChecksum(snap *matchingv1.Snapshot) error {
	if snap == nil {
		return ErrNilSnapshot
	}
	if ComputeChecksum(snap) != snap.GetChecksum() {
		return ErrChecksum
	}
	return nil
}

// Filename 返回 seq 对应的快照文件名，如 snapshot_100.pb。
func Filename(seqID uint64) string {
	return fmt.Sprintf("%s%d%s", snapshotPrefix, seqID, snapshotSuffix)
}

// Prune 保留 dir 下最新的 keep 份 snapshot_*.pb，删除更旧的文件。
func Prune(dir string, keep int) error {
	if keep <= 0 {
		keep = DefaultKeep
	}
	paths, err := listSnapshotPaths(dir)
	if err != nil {
		return err
	}
	if len(paths) <= keep {
		return nil
	}

	sort.Slice(paths, func(i, j int) bool {
		return parseSnapshotSeq(filepath.Base(paths[i])) > parseSnapshotSeq(filepath.Base(paths[j]))
	})
	for _, path := range paths[keep:] {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("snapshot: remove %q: %w", path, err)
		}
	}
	return nil
}

// LatestPath 返回 dir 下 seq 最大的快照文件路径；不存在时 ok=false。
func LatestPath(dir string) (string, bool, error) {
	paths, err := listSnapshotPaths(dir)
	if err != nil {
		return "", false, err
	}
	if len(paths) == 0 {
		return "", false, nil
	}
	sort.Slice(paths, func(i, j int) bool {
		return parseSnapshotSeq(filepath.Base(paths[i])) < parseSnapshotSeq(filepath.Base(paths[j]))
	})
	return paths[len(paths)-1], true, nil
}

func hashPriceLevel(h hashWriter, level *matchingv1.PriceLevel) {
	if level == nil {
		return
	}
	data, err := proto.Marshal(level)
	if err != nil {
		return
	}
	_, _ = h.Write(data)
}

type hashWriter interface {
	Write(p []byte) (n int, err error)
}

func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("snapshot: mkdir %q: %w", dir, err)
	}

	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("snapshot: create tmp %q: %w", tmpPath, err)
	}

	writeErr := func(err error) error {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	if _, err := f.Write(data); err != nil {
		return writeErr(fmt.Errorf("snapshot: write tmp: %w", err))
	}
	if err := f.Sync(); err != nil {
		return writeErr(fmt.Errorf("snapshot: fsync tmp: %w", err))
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("snapshot: close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("snapshot: rename: %w", err)
	}
	return nil
}

func listSnapshotPaths(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, snapshotPrefix+"*"+snapshotSuffix))
	if err != nil {
		return nil, fmt.Errorf("snapshot: glob %q: %w", dir, err)
	}
	return matches, nil
}

func parseSnapshotSeq(name string) uint64 {
	if !strings.HasPrefix(name, snapshotPrefix) || !strings.HasSuffix(name, snapshotSuffix) {
		return 0
	}
	numStr := strings.TrimSuffix(strings.TrimPrefix(name, snapshotPrefix), snapshotSuffix)
	seq, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0
	}
	return seq
}
