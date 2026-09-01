// convstore.go — P1 会话连续性注册表。
//
// 目标：对外保持 OpenAI 无状态（客户端每次发全量 messages），对内把
// 「前缀相同的连续请求」复用到同一个云端 remote 会话上，只把增量消息
// append 进云端会话（POST /api/remote/v1/chat_sessions/{id}/messages），
// 而不是每次新建会话 + 全量历史塞进一条 query。显著省 Work 积分（每个
// 新会话的重复历史不再重复计费），也让多轮对话在云端有真正的上下文连续。
//
// 键设计：messages 前缀哈希链 —— hash_{i} = H(hash_{i-1}, marshal(messages[i]))，
// 其中 hash_0 = H(model-version, marshal(messages[0]))。这样同一条对话线上的
// 第 N 轮请求天然拥有与第 N-1 轮相同的前缀哈希，可直接查表；用户中途改写
// 历史（edit/regenerate/换分支）则前缀哈希不同，自动走新建。模型名参与
// hash_0，防止不同模型串会话。
//
// 失效处理：
//   - 云端复用失败（发消息 4xx/5xx 等）→ 由调用方 Unbind 该前缀并退回
//     「新建会话 + 全量历史」路径（降级不劣于现状）；
//   - 空闲超过 TTL（默认 24h）→ sweeper 删除注册项并调 RemoteDeleteSession；
//   - 服务重启 → 从 data/conversations.json 恢复（tmp+rename 原子写盘）。
//
// 并发：per-conversation 互斥锁串行化同会话请求（复用会话时同一云端会话
// 不允许并发 append）；注册表本身一把全局锁。
package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// convEntry 一条可复用的云端会话注册项。
type convEntry struct {
	// CloudSessionID 云端 chat_session_id。
	CloudSessionID string `json:"cloud_session_id"`
	// UID 绑定账号（P2 粘性路由：复用必须回创建它的账号）。
	UID string `json:"uid"`
	// PrefixKey 本会话已消费的 messages 前缀哈希链终值。
	PrefixKey string `json:"prefix_key"`
	// Consumed 已 append 进云端会话的 messages 条数。
	Consumed int `json:"consumed"`
	// LastActive 最后一次成功使用时间（TTL 依据）。
	LastActive time.Time `json:"-"`
	// LastActiveUnix LastActive 的 Unix 秒（JSON 序列化用，LastActive 由它恢复）。
	LastActiveUnix int64 `json:"last_active_unix"`
	// LastReplyTail 上一轮 assistant 回复尾迹（回显校验锚点）。
	LastReplyTail string `json:"last_reply_tail,omitempty"`
}

// convStore 会话注册表：内存 map + 落盘 JSON。
type convStore struct {
	mu   sync.Mutex
	m    map[string]*convEntry // key: prefixKey（已消费前缀的哈希链终值）
	locks map[string]*sync.Mutex // per-conversation 串行化锁（key: cloudSessionID）
	fp   string                // 落盘路径；空 = 不持久化（单测）
}

func newConvStore(fp string) *convStore {
	s := &convStore{m: map[string]*convEntry{}, locks: map[string]*sync.Mutex{}, fp: fp}
	if fp != "" {
		s.load()
	}
	return s
}

// prefixChain 计算消息前缀哈希链，返回每条消息位置（0..n-1 已消费前缀）
// 对应的链终值。hash_0 混入模型名（带 maxMode 标记）隔离不同模型/模式的
// 会话；hash_{i} = H(hash_{i-1} + "|" + marshal(msg))。
// 返回长度 len(messages) 的数组：prefix[i] = 前 i+1 条消息的链哈希。
func convPrefixChain(model string, maxMode bool, messages []map[string]any) []string {
	h := sha256.Sum256([]byte("tw2api-conv-v1|" + model + "|max=" + boolStr(maxMode)))
	chain := make([]string, 0, len(messages))
	for _, msg := range messages {
		raw, _ := json.Marshal(msg)
		sum := sha256.Sum256(append(h[:], raw...))
		chain = append(chain, hex.EncodeToString(sum[:]))
		h = sum
	}
	return chain
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// Lookup 按「请求前缀链」找最长已知匹配。
// 请求 messages 的前缀链 prefix[0..n-1]；注册表里存的 key 是
// prefix[consumed-1]（已消费前缀的终值）。从最长前缀往短找，第一个
// 命中的注册项即最长匹配。
// 返回 (entry, 增量起始下标增量消息从 messages[incFrom:] 起需要 append)。
// incFrom = 命中项的 consumed；未命中时 incFrom = 0（全量重建）。
func (s *convStore) Lookup(model string, maxMode bool, messages []map[string]any) (*convEntry, int) {
	chain := convPrefixChain(model, maxMode, messages)
	s.mu.Lock()
	defer s.mu.Unlock()
	// 从最长前缀开始匹配（最长优先：多次 append 后链更深）。
	for i := len(chain) - 1; i >= 0; i-- {
		if e, ok := s.m[chain[i]]; ok {
			return e, i + 1
		}
	}
	return nil, 0
}

// Bind 登记/更新：以已消费前缀终值 prefixKey 为键注册会话。
// replyTail 为本轮 assistant 回复尾迹（下一轮回显校验锚点；可为空）。
// 同一 CloudSessionID 的所有旧前缀键同步刷新活跃时间与尾迹（多轮链上
// 中间键仍是活会话的有效入口）。
func (s *convStore) Bind(cloudSessionID, uid, prefixKey string, consumed int, replyTail string) {
	if cloudSessionID == "" || prefixKey == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	// 刷新同会话旧键的活跃时间（replyTail 有值时才覆盖：避免把新尾迹刷丢）。
	for _, e := range s.m {
		if e.CloudSessionID == cloudSessionID {
			e.LastActive = now
			e.LastActiveUnix = now.Unix()
			if replyTail != "" {
				e.LastReplyTail = replyTail
			}
		}
	}
	s.m[prefixKey] = &convEntry{
		CloudSessionID: cloudSessionID,
		UID:            uid,
		PrefixKey:      prefixKey,
		Consumed:       consumed,
		LastActive:     now,
		LastActiveUnix: now.Unix(),
		LastReplyTail:  replyTail,
	}
	s.saveLocked()
}

// Unbind 按云端会话 ID 删除所有指向它的注册项（复用失败时调用，防后续
// 请求继续往死会话上 append）。
func (s *convStore) Unbind(cloudSessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.m {
		if e.CloudSessionID == cloudSessionID {
			delete(s.m, k)
		}
	}
}

// LockFor 返回 per-conversation 串行化锁（同一云端会话的所有请求共用）。
func (s *convStore) LockFor(cloudSessionID string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.locks[cloudSessionID]; ok {
		return l
	}
	l := &sync.Mutex{}
	s.locks[cloudSessionID] = l
	return l
}

// lockKeyOf 已废弃（serveRemote 直接用命中项的 CloudSessionID 取锁），
// 保留空实现以兼容旧引用。
func (s *convStore) lockKeyOf(prefixKey string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.m[prefixKey]; ok {
		return e.CloudSessionID
	}
	return ""
}

// Sweep 清理空闲超 TTL 的注册项并删除对应云端会话（释放并发槽）。
// del 为删除云端会话的回调（cloudSessionID, uid）；由调用方注入以免
// convstore 反向依赖 upstream。
func (s *convStore) Sweep(ttl time.Duration, del func(cloudSessionID, uid string)) {
	cut := time.Now().Add(-ttl)
	s.mu.Lock()
	var expired []*convEntry
	for k, e := range s.m {
		last := e.LastActive
		if last.IsZero() {
			last = time.Unix(e.LastActiveUnix, 0)
		}
		if last.Before(cut) {
			expired = append(expired, e)
			delete(s.m, k)
		}
	}
	s.mu.Unlock()
	for _, e := range expired {
		if del != nil {
			del(e.CloudSessionID, e.UID)
		}
	}
	if len(expired) > 0 {
		s.save()
	}
}

// ---------------------------------------------------------------------------
// 持久化（tmp + rename 原子写）
// ---------------------------------------------------------------------------

type convFile struct {
	Conversations map[string]*convEntry `json:"conversations"`
}

func (s *convStore) save() {
	if s.fp == "" {
		return
	}
	s.mu.Lock()
	s.saveLocked()
	s.mu.Unlock()
}

// saveLocked 持锁写盘（Bind 已持锁时直接调用）。
func (s *convStore) saveLocked() {
	if s.fp == "" {
		return
	}
	doc := convFile{Conversations: map[string]*convEntry{}}
	for k, e := range s.m {
		e.LastActiveUnix = e.LastActive.Unix()
		doc.Conversations[k] = e
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return
	}
	if dir := filepath.Dir(s.fp); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	tmp := s.fp + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		log.Printf("convstore save: %v", err)
		return
	}
	if err := os.Rename(tmp, s.fp); err != nil {
		log.Printf("convstore rename: %v", err)
	}
}

func (s *convStore) load() {
	raw, err := os.ReadFile(s.fp)
	if err != nil {
		return
	}
	var doc convFile
	if json.Unmarshal(raw, &doc) != nil {
		return
	}
	for k, e := range doc.Conversations {
		if e == nil || e.CloudSessionID == "" {
			continue
		}
		if e.LastActive.IsZero() && e.LastActiveUnix > 0 {
			e.LastActive = time.Unix(e.LastActiveUnix, 0)
		}
		s.m[k] = e
	}
}
