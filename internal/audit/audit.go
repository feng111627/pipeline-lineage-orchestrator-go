// Package audit 记录每次执行的成功失败、耗时与分类后的失败原因。
// 本地 JSONL 追加式存储；Message 只含对外安全信息，不透出底层错误细节。
package audit

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/errs"
)

// FailureKind 区分失败原因类别。
type FailureKind string

const (
	FailureNone       FailureKind = ""
	FailureValidation FailureKind = "validation"
	FailureExecution  FailureKind = "execution"
	FailureConflict   FailureKind = "conflict"
	FailureNotFound   FailureKind = "not_found"
)

// Entry 是一条执行审计记录。
type Entry struct {
	RunID     string        `json:"run_id"`
	Node      string        `json:"node"`
	Status    string        `json:"status"` // succeeded / failed / skipped
	Attempts  int           `json:"attempts"`
	Duration  time.Duration `json:"duration_ns"`
	Failure   FailureKind   `json:"failure,omitempty"`
	Message   string        `json:"message,omitempty"` // 对外安全信息
	StartedAt time.Time     `json:"started_at"`
}

// Log 是本地追加式审计日志。
type Log struct {
	dir string
	mu  sync.Mutex
}

// Open 打开（必要时创建）审计日志。
func Open(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, errs.Wrap(errs.KindUnknown, "audit.Open", "无法创建审计日志目录", err)
	}
	return &Log{dir: dir}, nil
}

func (l *Log) path() string { return filepath.Join(l.dir, "audit.jsonl") }

// Append 追加一条审计记录。
func (l *Log) Append(e Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, err := json.Marshal(e)
	if err != nil {
		return errs.Wrap(errs.KindUnknown, "audit.Append", "无法序列化审计记录", err)
	}
	f, err := os.OpenFile(l.path(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return errs.Wrap(errs.KindUnknown, "audit.Append", "无法打开审计日志文件", err)
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		return errs.Wrap(errs.KindUnknown, "audit.Append", "无法写入审计记录", err)
	}
	return nil
}

// List 返回某次运行的全部审计记录；runID 为空时返回全部。
func (l *Log) List(runID string) ([]Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.Open(l.path())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errs.Wrap(errs.KindUnknown, "audit.List", "无法读取审计日志", err)
	}
	defer f.Close()
	var out []Entry
	dec := json.NewDecoder(f)
	for dec.More() {
		var e Entry
		if err := dec.Decode(&e); err != nil {
			return nil, errs.Wrap(errs.KindUnknown, "audit.List", "审计日志内容损坏", err)
		}
		if runID == "" || e.RunID == runID {
			out = append(out, e)
		}
	}
	return out, nil
}
