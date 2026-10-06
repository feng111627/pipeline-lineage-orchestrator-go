// Package version 提供基于本地文件系统的数据版本管理：
// 产出（幂等）、晋升、回滚与读取。不依赖任何外部服务。
package version

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 对上层暴露的语义化错误；底层文件错误不原样透出，统一包装为语义错误。
var (
	ErrNotFound     = errors.New("version: dataset or version not found")
	ErrConflict     = errors.New("version: conflict, current state does not match expectation")
	ErrInvalidInput = errors.New("version: invalid input")
	ErrStorage      = errors.New("version: local storage operation failed")
)

// Version 描述一个数据版本的元信息。
type Version struct {
	Dataset     string `json:"dataset"`
	Seq         int    `json:"seq"`         // 从 1 开始的单调序号
	Fingerprint string `json:"fingerprint"` // 输入+配置+代码版本的指纹，用于幂等
	Promoted    bool   `json:"promoted"`    // 是否已晋升（发布）
	CreatedAt   int64  `json:"created_at"`  // Unix 秒
}

// meta 是单个数据集的元数据文件内容。
type meta struct {
	Versions []Version `json:"versions"`
	Current  int       `json:"current"` // 当前生效版本序号，0 表示尚未生效
}

// Store 是基于本地目录的版本存储，单进程内并发安全。
type Store struct {
	root string
	mu   sync.Mutex
}

// Open 打开（必要时创建）位于 root 的版本存储。
func Open(root string) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("%w: empty root dir", ErrInvalidInput)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("%w: cannot prepare root dir", ErrStorage)
	}
	return &Store{root: root}, nil
}

// Produce 写入一份数据。若同 dataset 已存在相同 fingerprint 的版本，
// 直接返回该版本而不产生新版本（幂等）。新产出的版本立即成为当前生效版本。
func (s *Store) Produce(dataset, fingerprint string, data []byte) (Version, error) {
	if dataset == "" || fingerprint == "" {
		return Version{}, fmt.Errorf("%w: dataset and fingerprint must not be empty", ErrInvalidInput)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	m, err := s.loadMeta(dataset)
	if err != nil {
		return Version{}, err
	}
	// 幂等：相同指纹直接返回已有版本
	for _, v := range m.Versions {
		if v.Fingerprint == fingerprint {
			return v, nil
		}
	}
	v := Version{
		Dataset:     dataset,
		Seq:         len(m.Versions) + 1,
		Fingerprint: fingerprint,
		CreatedAt:   time.Now().Unix(),
	}
	if err := s.writeBlob(dataset, v.Seq, data); err != nil {
		return Version{}, err
	}
	m.Versions = append(m.Versions, v)
	m.Current = v.Seq // 新产出版本立即生效
	if err := s.saveMeta(dataset, m); err != nil {
		return Version{}, err
	}
	return v, nil
}

// Promote 将指定版本晋升为发布态并置为当前生效版本；
// 若 expectCurrent 与当前生效版本不一致，返回 ErrConflict，不做任何修改。
func (s *Store) Promote(dataset string, seq int, expectCurrent int) (Version, error) {
	return s.moveCurrent(dataset, seq, expectCurrent, true)
}

// Rollback 将当前生效版本回滚到历史版本 seq；
// expectCurrent 校验失败返回 ErrConflict，不做任何修改。
func (s *Store) Rollback(dataset string, seq int, expectCurrent int) (Version, error) {
	return s.moveCurrent(dataset, seq, expectCurrent, false)
}

// moveCurrent 是晋升/回滚的公共实现：乐观校验 expectCurrent 后切换当前版本。
func (s *Store) moveCurrent(dataset string, seq int, expectCurrent int, markPromoted bool) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, err := s.loadMeta(dataset)
	if err != nil {
		return Version{}, err
	}
	if m.Current != expectCurrent {
		return Version{}, fmt.Errorf("%w: dataset %q current is %d, expected %d",
			ErrConflict, dataset, m.Current, expectCurrent)
	}
	idx := -1
	for i, v := range m.Versions {
		if v.Seq == seq {
			idx = i
			break
		}
	}
	if idx < 0 {
		return Version{}, fmt.Errorf("%w: dataset %q version %d", ErrNotFound, dataset, seq)
	}
	if markPromoted {
		m.Versions[idx].Promoted = true
	}
	m.Current = seq
	if err := s.saveMeta(dataset, m); err != nil {
		return Version{}, err
	}
	return m.Versions[idx], nil
}

// Current 返回数据集当前生效的版本。
func (s *Store) Current(dataset string) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.loadMeta(dataset)
	if err != nil {
		return Version{}, err
	}
	for _, v := range m.Versions {
		if v.Seq == m.Current {
			return v, nil
		}
	}
	return Version{}, fmt.Errorf("%w: dataset %q has no current version", ErrNotFound, dataset)
}

// Get 读取指定版本的数据内容。
func (s *Store) Get(dataset string, seq int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.loadMeta(dataset)
	if err != nil {
		return nil, err
	}
	found := false
	for _, v := range m.Versions {
		if v.Seq == seq {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("%w: dataset %q version %d", ErrNotFound, dataset, seq)
	}
	b, err := os.ReadFile(s.blobPath(dataset, seq))
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read version content", ErrStorage)
	}
	return b, nil
}

// List 返回数据集的全部版本，按序号升序。
func (s *Store) List(dataset string) ([]Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.loadMeta(dataset)
	if err != nil {
		return nil, err
	}
	return append([]Version{}, m.Versions...), nil
}

// --- 内部存储辅助 ---

// checkDataset 拒绝空名与可能逃逸根目录的非法数据集名。
func checkDataset(dataset string) error {
	if dataset == "" || dataset == "." || dataset == ".." ||
		filepath.Base(dataset) != dataset {
		return fmt.Errorf("%w: illegal dataset name %q", ErrInvalidInput, dataset)
	}
	return nil
}

func (s *Store) datasetDir(dataset string) string {
	return filepath.Join(s.root, filepath.Clean(dataset))
}

func (s *Store) blobPath(dataset string, seq int) string {
	return filepath.Join(s.datasetDir(dataset), fmt.Sprintf("v%d.data", seq))
}

func (s *Store) metaPath(dataset string) string {
	return filepath.Join(s.datasetDir(dataset), "meta.json")
}

func (s *Store) loadMeta(dataset string) (*meta, error) {
	if err := checkDataset(dataset); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(s.metaPath(dataset))
	if errors.Is(err, os.ErrNotExist) {
		return &meta{}, nil // 新数据集
	}
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read metadata", ErrStorage)
	}
	var m meta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("%w: corrupted metadata", ErrStorage)
	}
	return &m, nil
}

// saveMeta 先写临时文件再原子改名，避免半截元数据。
func (s *Store) saveMeta(dataset string, m *meta) error {
	if err := os.MkdirAll(s.datasetDir(dataset), 0o755); err != nil {
		return fmt.Errorf("%w: cannot prepare dataset dir", ErrStorage)
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: cannot encode metadata", ErrStorage)
	}
	tmp := s.metaPath(dataset) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("%w: cannot write metadata", ErrStorage)
	}
	if err := os.Rename(tmp, s.metaPath(dataset)); err != nil {
		return fmt.Errorf("%w: cannot commit metadata", ErrStorage)
	}
	return nil
}

func (s *Store) writeBlob(dataset string, seq int, data []byte) error {
	if err := os.MkdirAll(s.datasetDir(dataset), 0o755); err != nil {
		return fmt.Errorf("%w: cannot prepare dataset dir", ErrStorage)
	}
	if err := os.WriteFile(s.blobPath(dataset, seq), data, 0o644); err != nil {
		return fmt.Errorf("%w: cannot write version content", ErrStorage)
	}
	return nil
}
