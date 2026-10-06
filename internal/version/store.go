// Package version 提供本地数据版本管理：产出、晋升、回滚。
// 全部状态落在本地目录的 JSON 文件上，不依赖任何外部服务。
package version

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/errs"
)

// Version 是一次数据产出的不可变版本。
type Version struct {
	ID        string    `json:"id"`
	Artifact  string    `json:"artifact"`
	InputHash string    `json:"input_hash"`
	DataHash  string    `json:"data_hash"`
	Data      []byte    `json:"data"`
	CreatedAt time.Time `json:"created_at"`
}

// Channel 是版本晋升通道（如 "current"）。
type Channel struct {
	Name      string    `json:"name"`
	VersionID string    `json:"version_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Store 是本地版本存储。进程内用互斥锁保证并发安全，写盘采用临时文件+重命名保证原子性。
type Store struct {
	dir string
	mu  sync.Mutex
}

// Open 打开（必要时创建）位于 dir 的版本存储。
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, errs.Wrap(errs.KindUnknown, "version.Open", "无法创建版本存储目录", err)
	}
	return &Store{dir: dir}, nil
}

func hashOf(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// versionID 由输入指纹与内容哈希共同派生：同输入同内容 => 同版本号（幂等）。
func versionID(inputHash, dataHash string) string {
	return "v-" + hashOf([]byte(inputHash), []byte(dataHash))[:16]
}

func (s *Store) artifactDir(a string) string  { return filepath.Join(s.dir, "artifacts", a) }
func (s *Store) versionPath(a, id string) string {
	return filepath.Join(s.artifactDir(a), "versions", id+".json")
}
func (s *Store) channelPath(a, c string) string {
	return filepath.Join(s.artifactDir(a), "channels", c+".json")
}
func (s *Store) indexPath(a string) string {
	return filepath.Join(s.artifactDir(a), "inputs.json")
}

func writeJSONAtomic(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// Commit 产出版本。
// 返回 (version, created, err)：
//   - 同一产物+同一输入指纹+同一内容 => 返回已存在版本，created=false，不产生重复版本；
//   - 同一输入指纹但内容不同 => 返回冲突错误，绝不默默覆盖。
func (s *Store) Commit(artifact, inputHash string, data []byte) (*Version, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dataHash := hashOf(data)
	id := versionID(inputHash, dataHash)

	// 幂等：版本已存在则直接返回
	if existing, err := s.getLocked(artifact, id); err == nil {
		return existing, false, nil
	}

	// 冲突：同一输入指纹已对应其他版本（内容不一致）
	index := map[string]string{}
	if err := readJSON(s.indexPath(artifact), &index); err != nil && !os.IsNotExist(err) {
		return nil, false, errs.Wrap(errs.KindUnknown, "version.Commit", "无法读取输入指纹索引", err)
	}
	if prevID, ok := index[inputHash]; ok && prevID != id {
		return nil, false, errs.Wrap(errs.KindConflict, "version.Commit",
			fmt.Sprintf("产物 %q 的输入指纹 %q 已对应版本 %s，本次内容不一致，拒绝覆盖",
				artifact, inputHash, prevID), nil)
	}

	v := &Version{
		ID: id, Artifact: artifact, InputHash: inputHash,
		DataHash: dataHash, Data: data, CreatedAt: time.Now().UTC(),
	}
	if err := writeJSONAtomic(s.versionPath(artifact, id), v); err != nil {
		return nil, false, errs.Wrap(errs.KindUnknown, "version.Commit", "无法写入版本文件", err)
	}
	index[inputHash] = id
	if err := writeJSONAtomic(s.indexPath(artifact), index); err != nil {
		return nil, false, errs.Wrap(errs.KindUnknown, "version.Commit", "无法更新输入指纹索引", err)
	}
	return v, true, nil
}

func (s *Store) getLocked(artifact, versionID string) (*Version, error) {
	var v Version
	if err := readJSON(s.versionPath(artifact, versionID), &v); err != nil {
		if os.IsNotExist(err) {
			return nil, errs.Wrap(errs.KindNotFound, "version.Get",
				fmt.Sprintf("产物 %q 不存在版本 %s", artifact, versionID), err)
		}
		return nil, errs.Wrap(errs.KindUnknown, "version.Get", "无法读取版本文件", err)
	}
	return &v, nil
}

// Get 读取指定版本。
func (s *Store) Get(artifact, versionID string) (*Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getLocked(artifact, versionID)
}

func (s *Store) readChannelLocked(artifact, channel string) (*Channel, error) {
	var c Channel
	if err := readJSON(s.channelPath(artifact, channel), &c); err != nil {
		if os.IsNotExist(err) {
			return nil, nil // 通道尚不存在
		}
		return nil, errs.Wrap(errs.KindUnknown, "version.readChannel", "无法读取通道文件", err)
	}
	return &c, nil
}

// setChannelLocked 带乐观并发检查地写通道：expectCurrent 为 "" 表示要求通道不存在；
// 否则必须等于通道当前版本，不一致即冲突，绝不默默覆盖。
func (s *Store) setChannelLocked(artifact, channel, versionID, expectCurrent, op string) error {
	cur, err := s.readChannelLocked(artifact, channel)
	if err != nil {
		return err
	}
	switch {
	case cur == nil && expectCurrent != "":
		return errs.Wrap(errs.KindConflict, op,
			fmt.Sprintf("通道 %q 尚不存在，无法满足期望当前版本 %q", channel, expectCurrent), nil)
	case cur != nil && cur.VersionID != expectCurrent:
		return errs.Wrap(errs.KindConflict, op,
			fmt.Sprintf("通道 %q 当前版本为 %s，与期望的 %q 不一致，拒绝操作",
				channel, cur.VersionID, expectCurrent), nil)
	case cur != nil && cur.VersionID == versionID:
		return nil // 幂等：已指向目标版本
	}
	c := &Channel{Name: channel, VersionID: versionID, UpdatedAt: time.Now().UTC()}
	if err := writeJSONAtomic(s.channelPath(artifact, channel), c); err != nil {
		return errs.Wrap(errs.KindUnknown, op, "无法写入通道文件", err)
	}
	return nil
}

// Promote 把某版本晋升到通道。expectCurrent 为乐观并发期望值：
// 通道已被其他版本占用且期望不符时报冲突错误。
func (s *Store) Promote(artifact, channel, versionID, expectCurrent string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.getLocked(artifact, versionID); err != nil {
		return err
	}
	return s.setChannelLocked(artifact, channel, versionID, expectCurrent, "version.Promote")
}

// Rollback 把通道回滚到历史版本。必须显式给出 expectCurrent（当前期望版本），
// 不一致则冲突报错；回滚成功后查询结果与历史版本当时完全一致（版本数据不可变）。
func (s *Store) Rollback(artifact, channel, toVersionID, expectCurrent string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.getLocked(artifact, toVersionID); err != nil {
		return err
	}
	return s.setChannelLocked(artifact, channel, toVersionID, expectCurrent, "version.Rollback")
}

// Current 返回通道当前指向的版本。
func (s *Store) Current(artifact, channel string) (*Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, err := s.readChannelLocked(artifact, channel)
	if err != nil {
		return nil, err
	}
	if cur == nil {
		return nil, errs.Wrap(errs.KindNotFound, "version.Current",
			fmt.Sprintf("产物 %q 的通道 %q 尚未指向任何版本", artifact, channel), nil)
	}
	return s.getLocked(artifact, cur.VersionID)
}
