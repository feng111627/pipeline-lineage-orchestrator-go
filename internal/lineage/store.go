// Package lineage 记录产物血缘：某版本由哪些输入、哪份配置与代码版本、哪次运行产出。
// 本地 JSONL 追加式存储，不依赖外部服务。
package lineage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/errs"
)

// Record 是一条血缘记录。
type Record struct {
	Artifact    string    `json:"artifact"`
	VersionID   string    `json:"version_id"`
	Inputs      []string  `json:"inputs"` // 上游产物版本，形如 "artifact@version"
	ConfigHash  string    `json:"config_hash"`
	CodeVersion string    `json:"code_version"`
	RunID       string    `json:"run_id"`
	CreatedAt   time.Time `json:"created_at"`
}

// Store 是本地追加式血缘存储。
type Store struct {
	dir string
	mu  sync.Mutex
}

// Open 打开（必要时创建）血缘存储。
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, errs.Wrap(errs.KindUnknown, "lineage.Open", "无法创建血缘存储目录", err)
	}
	return &Store{dir: dir}, nil
}

func (s *Store) path() string { return filepath.Join(s.dir, "lineage.jsonl") }

// Add 追加一条血缘记录。同一产物同一版本重复追加会被去重（幂等）。
func (s *Store) Add(r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, err := s.ofLocked(r.Artifact, r.VersionID); err == nil && existing != nil {
		return nil // 已存在，幂等返回
	}
	b, err := json.Marshal(r)
	if err != nil {
		return errs.Wrap(errs.KindUnknown, "lineage.Add", "无法序列化血缘记录", err)
	}
	f, err := os.OpenFile(s.path(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return errs.Wrap(errs.KindUnknown, "lineage.Add", "无法打开血缘存储文件", err)
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		return errs.Wrap(errs.KindUnknown, "lineage.Add", "无法写入血缘记录", err)
	}
	return nil
}

func (s *Store) ofLocked(artifact, versionID string) (*Record, error) {
	f, err := os.Open(s.path())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errs.Wrap(errs.KindNotFound, "lineage.Of",
				fmt.Sprintf("产物 %q 版本 %s 没有血缘记录", artifact, versionID), err)
		}
		return nil, errs.Wrap(errs.KindUnknown, "lineage.Of", "无法读取血缘存储", err)
	}
	defer f.Close()
	var found *Record
	dec := json.NewDecoder(f)
	for dec.More() {
		var r Record
		if err := dec.Decode(&r); err != nil {
			return nil, errs.Wrap(errs.KindUnknown, "lineage.Of", "血缘存储内容损坏", err)
		}
		if r.Artifact == artifact && r.VersionID == versionID {
			cp := r
			found = &cp
		}
	}
	if found == nil {
		return nil, errs.Wrap(errs.KindNotFound, "lineage.Of",
			fmt.Sprintf("产物 %q 版本 %s 没有血缘记录", artifact, versionID), nil)
	}
	return found, nil
}

// Of 查询某产物某版本的血缘。
func (s *Store) Of(artifact, versionID string) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ofLocked(artifact, versionID)
}

// String 以可读形式展示一条血缘记录，便于日志与文档示例。
func (r Record) String() string {
	return fmt.Sprintf("%s@%s <- inputs=[%s] config=%s code=%s run=%s",
		r.Artifact, r.VersionID, strings.Join(r.Inputs, ","), r.ConfigHash, r.CodeVersion, r.RunID)
}
