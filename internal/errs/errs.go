// Package errs 定义统一的错误分类，避免底层错误原样透传给上层调用方。
package errs

import (
	"errors"
	"fmt"
)

// Kind 表示错误的业务分类。
type Kind int

const (
	KindUnknown Kind = iota
	KindValidation // 输入校验失败（如依赖成环）
	KindConflict   // 版本晋升/回滚冲突
	KindNotFound   // 版本或产物不存在
	KindExecution  // 节点执行失败
)

func (k Kind) String() string {
	switch k {
	case KindValidation:
		return "validation"
	case KindConflict:
		return "conflict"
	case KindNotFound:
		return "not_found"
	case KindExecution:
		return "execution"
	default:
		return "unknown"
	}
}

// Error 是带分类与对外安全信息的错误。
type Error struct {
	Kind    Kind
	Op      string // 出错的业务操作
	Message string // 对上层暴露的安全信息
	cause   error  // 底层原因，不直接透出
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s: %s", e.Op, e.Kind, e.Message)
}

// Unwrap 暴露底层原因供 errors.Is/As 内部判断，但 Error() 不含其细节。
func (e *Error) Unwrap() error { return e.cause }

// Cause 返回底层原因（仅供内部日志使用）。
func (e *Error) Cause() error { return e.cause }

// Wrap 把底层错误包装为分类错误；cause 可为 nil。
func Wrap(kind Kind, op, msg string, cause error) *Error {
	return &Error{Kind: kind, Op: op, Message: msg, cause: cause}
}

// IsKind 判断错误链中是否包含指定分类。
func IsKind(err error, kind Kind) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind == kind
	}
	return false
}
