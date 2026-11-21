package runtime

import (
	"context"
	"errors"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// Handle 直接提供 go-redis 常规命令和管道，保留原生返回值及 redis.Nil 语义。
// 不提供 Close、Options、AddHook 等连接池管理能力。
type Handle struct {
	redis.Cmdable
	manager   *Manager
	client    *redis.Client
	keyPrefix string
}

// Key 拼接应用配置的前缀和业务键，分隔符由配置中的前缀决定。
func (h *Handle) Key(key string) string { return h.keyPrefix + key }

// WithDataSource 创建独立的具名数据源句柄，不修改默认值或当前句柄。
func (h *Handle) WithDataSource(name string) (*Handle, error) {
	if h == nil || h.manager == nil {
		return nil, errors.New("cache: 数据源句柄无效")
	}
	m := h.manager
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.usable(); err != nil {
		return nil, err
	}
	if _, ok := m.clients[name]; !ok {
		return nil, errors.New("cache: 指定数据源不存在")
	}
	return m.handle(name), nil
}

// Do 执行 SDK 未提供专用方法的命令，认证及服务器支持范围由当前数据源决定。
func (h *Handle) Do(ctx context.Context, args ...any) *redis.Cmd {
	if h == nil || h.client == nil {
		return redis.NewCmdResult(nil, errUnavailable)
	}
	return h.client.Do(ctx, args...)
}

var (
	errUnavailable      = errors.New("cache: 尚未启用、尚未启动或已关闭，请在应用启动后获取句柄")
	unavailableOnce     sync.Once
	unavailableCommands redis.Cmdable
)

// unavailableHandle 用已关闭且永远拒绝执行的 SDK 命令分发器返回原生类型错误，绝不尝试默认地址。
func unavailableHandle(manager *Manager) *Handle {
	unavailableOnce.Do(func() {
		client := redis.NewClient(&redis.Options{Addr: "invalid.invalid:1", Protocol: 2, DisableIdentity: true, PoolSize: 1, MaxRetries: -1,
			MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}})
		client.AddHook(lifecycleHook{})
		_ = client.Close()
		unavailableCommands = client
	})
	return &Handle{Cmdable: unavailableCommands, manager: manager}
}
