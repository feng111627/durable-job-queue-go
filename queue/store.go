package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// store 负责队列状态的原子快照持久化。
//
// 取舍：每次状态变更后整体写快照（写临时文件 + fsync + rename），
// 单机模拟场景下实现简单、恢复逻辑确定；rename 保证崩溃时要么看到
// 旧快照要么看到新快照，不会出现半写状态。
type store struct {
	path string // 快照文件完整路径
}

func newStore(dir string) *store {
	return &store{path: filepath.Join(dir, "queue-snapshot.json")}
}

// save 将快照原子写入磁盘。
func (s *store) save(snap *snapshot) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("queue: mkdir: %w", err)
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("queue: marshal snapshot: %w", err)
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("queue: open tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("queue: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("queue: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("queue: close tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("queue: rename: %w", err)
	}
	return nil
}

// load 读取快照；文件不存在时返回 nil, nil。
func (s *store) load() (*snapshot, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("queue: read snapshot: %w", err)
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("queue: decode snapshot: %w", err)
	}
	return &snap, nil
}
