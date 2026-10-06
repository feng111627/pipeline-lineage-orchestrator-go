// Package lineage 记录数据血缘与执行审计，全部落盘在本地 JSONL 文件。
package lineage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 语义化错误；底层文件错误不原样透出。
var (
	ErrNotFound = errors.New("lineage: record not found")
	ErrStorage  = errors.New("lineage: local storage operation failed")
)

// FailureKind 区分失败原因类别。
type FailureKind string

const (
	FailureNone      FailureKind = ""
	FailureNodeLogic FailureKind = "node_logic" // 节点业务逻辑失败
	FailureRetryOut  FailureKind = "retry_exhausted"
	FailureSkipped   FailureKind = "skipped_upstream"
	FailureInternal  FailureKind = "internal" // 编排器内部错误
)

// Record 描述一次节点产出：谁、用什么输入、哪份配置/代码，产出了哪个版本。
type Record struct {
	Artifact    string   `json:"artifact"`     // 产物（数据集）名
	DatasetSeq  int      `json:"dataset_seq"`  // 产出的数据版本号
	Inputs      []string `json:"inputs"`       // 输入产物列表（dataset@seq）
	ConfigHash  string   `json:"config_hash"`  // 配置指纹
	CodeVersion string   `json:"code_version"` // 代码版本
	RunID       string   `json:"run_id"`       // 对应执行记录
	CreatedAt   int64    `json:"created_at"`
}

// ExecRecord 是一次节点执行的审计记录。
type ExecRecord struct {
	RunID    string        `json:"run_id"`
	Node     string        `json:"node"`
	Status   string        `json:"status"` // success / failed / skipped
	Kind     FailureKind   `json:"kind"`
	Attempts int           `json:"attempts"`
	Duration time.Duration `json:"duration"`
	Message  string        `json:"message"` // 脱敏后的失败说明
	At       int64         `json:"at"`
}

// Store 是本地血缘与审计存储，单进程内并发安全。
// path 为目录，内含 lineage.jsonl 与 exec.jsonl 两个追加式文件。
type Store struct {
	dir string
	mu  sync.Mutex
}

// Open 打开（必要时创建）位于 dir 的存储。
func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty dir", ErrStorage)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("%w: cannot prepare dir", ErrStorage)
	}
	return &Store{dir: dir}, nil
}

// AppendLineage 追加一条血缘记录。
func (s *Store) AppendLineage(r Record) error {
	if r.CreatedAt == 0 {
		r.CreatedAt = time.Now().Unix()
	}
	return s.appendJSONL("lineage.jsonl", r)
}

// AppendExec 追加一条执行审计记录。
func (s *Store) AppendExec(e ExecRecord) error {
	if e.At == 0 {
		e.At = time.Now().Unix()
	}
	return s.appendJSONL("exec.jsonl", e)
}

// LineageOf 查询某产物某版本的血缘记录。
func (s *Store) LineageOf(artifact string, seq int) (Record, error) {
	var found *Record
	err := s.scanJSONL("lineage.jsonl", func(line []byte) bool {
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			return true // 跳过坏行，继续扫描
		}
		if r.Artifact == artifact && r.DatasetSeq == seq {
			r := r
			found = &r
			return false
		}
		return true
	})
	if err != nil {
		return Record{}, err
	}
	if found == nil {
		return Record{}, fmt.Errorf("%w: %s@%d", ErrNotFound, artifact, seq)
	}
	return *found, nil
}

// ExecsOf 查询某次运行的全部执行记录，按写入顺序返回。
func (s *Store) ExecsOf(runID string) ([]ExecRecord, error) {
	var out []ExecRecord
	err := s.scanJSONL("exec.jsonl", func(line []byte) bool {
		var e ExecRecord
		if err := json.Unmarshal(line, &e); err != nil {
			return true
		}
		if e.RunID == runID {
			out = append(out, e)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// appendJSONL 以追加方式写入一行 JSON。
func (s *Store) appendJSONL(name string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(filepath.Join(s.dir, name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("%w: cannot open log", ErrStorage)
	}
	defer f.Close()
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("%w: cannot encode record", ErrStorage)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("%w: cannot append record", ErrStorage)
	}
	return nil
}

// scanJSONL 逐行扫描文件，fn 返回 false 时提前结束。文件不存在视为空。
func (s *Store) scanJSONL(name string, fn func(line []byte) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(filepath.Join(s.dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: cannot open log", ErrStorage)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		if !fn(sc.Bytes()) {
			break
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("%w: cannot read log", ErrStorage)
	}
	return nil
}
