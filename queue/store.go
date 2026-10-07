package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// store 负责把队列状态原子地落盘与恢复。
//
// 取舍：每次状态变更后整体快照落盘（tmp 文件 + rename 保证原子性），
// 单机模拟场景下实现简单、恢复逻辑确定（读到的就是最后一次完整快照），
// 代价是写入放大，不适合高吞吐场景。
type store struct {
	path string
}

// diskState 是持久化到磁盘的完整状态快照。
type diskState struct {
	Jobs   []*Job                             `json:"jobs"`
	Groups map[string]map[string]*deliveryRecord `json:"groups"` // group -> jobID -> record
	Dedup  *dedupState                        `json:"dedup"`
}

func newStore(path string) *store {
	return &store{path: path}
}

// save 将状态原子写入磁盘（先写临时文件再 rename，崩溃时不会留下半个文件）。
func (s *store) save(st *diskState) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("queue: marshal state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("queue: mkdir: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("queue: write tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("queue: rename: %w", err)
	}
	return nil
}

// load 从磁盘恢复状态；文件不存在时返回空状态。
func (s *store) load() (*diskState, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return &diskState{
			Groups: map[string]map[string]*deliveryRecord{},
			Dedup:  newDedupState(),
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("queue: read state: %w", err)
	}
	var st diskState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("queue: decode state: %w", err)
	}
	if st.Groups == nil {
		st.Groups = map[string]map[string]*deliveryRecord{}
	}
	if st.Dedup == nil {
		st.Dedup = newDedupState()
	}
	if st.Dedup.Entries == nil {
		st.Dedup.Entries = map[string]dedupEntry{}
	}
	return &st, nil
}
