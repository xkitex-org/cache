package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// Manager 持有一套应用缓存配置及独立连接池，不依赖 core 或 monitor。
type Manager struct {
	mu                          sync.RWMutex
	cfg                         settings
	clients                     map[string]*redis.Client
	configured, started, closed bool
	closeErr                    error
	closeDone                   chan struct{}
}

// New 只创建管理器，不构造 SDK 客户端、读取环境或启动协程。
func New() *Manager { return &Manager{closeDone: make(chan struct{})} }

// ConfigureCache 单次接收合并后的 cache 节，未配置或非法配置阻止启动。
func (m *Manager) ConfigureCache(body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.configured || m.closed {
		return errors.New("cache: 配置只允许在启动前应用一次")
	}
	cfg, err := parseSettings(body)
	if err != nil {
		return err
	}
	m.cfg, m.configured = cfg, true
	return nil
}

// Start 创建各数据源的独立连接池句柄，连通性由可选 readiness 检查确认。
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.configured || m.started || m.closed {
		return errors.New("cache: 启动生命周期无效")
	}
	if ctx == nil {
		return errors.New("cache: context 不能为空")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	installSDKLogger()
	clients := make(map[string]*redis.Client)
	for _, name := range names(m.cfg.Sources) {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, closeClients(clients))
		}
		source := m.cfg.Sources[name]
		client := redis.NewClient(&redis.Options{
			Addr: source.Address, Username: source.Username, Password: source.Password, DB: source.DB,
			Protocol: 2, DisableIdentity: true, ContextTimeoutEnabled: true, MaxRetries: -1,
			PoolSize: source.PoolSize, MaxActiveConns: source.PoolSize,
			DialTimeout: source.dialTimeout, ReadTimeout: source.readTimeout, WriteTimeout: source.writeTimeout, PoolTimeout: source.poolTimeout,
			MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		})
		client.AddHook(lifecycleHook{manager: m})
		clients[name] = client
		klog.Infof("cache: 连接池已创建：source=[%s] type=[%s]", name, source.Type)
	}
	m.clients, m.started = clients, true
	return nil
}

// Cache 返回默认数据源的命令句柄；启动前或关闭后取得的句柄只会返回生命周期错误。
func (m *Manager) Cache() *Handle {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.usable(); err != nil {
		return unavailableHandle(m)
	}
	return m.handle(m.cfg.Default)
}

// handle 在持有管理器锁时创建具名句柄，不暴露连接池关闭方法。
func (m *Manager) handle(name string) *Handle {
	client := m.clients[name]
	return &Handle{Cmdable: client, client: client, manager: m, keyPrefix: m.cfg.KeyPrefix}
}

// ReadinessChecks 返回每个数据源的独立 Ping，不启动定时器或其他后台检查。
func (m *Manager) ReadinessChecks() map[string]func(context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	checks := make(map[string]func(context.Context) error, len(m.clients))
	for name := range m.clients {
		checks["cache."+name] = func(ctx context.Context) error { return m.ping(ctx, name) }
	}
	return checks
}

// ping 遵循 monitor 更短的截止时间，同时限制数据源级别 Ping 上限。
func (m *Manager) ping(ctx context.Context, name string) error {
	if ctx == nil {
		return errors.New("cache: context 不能为空")
	}
	m.mu.RLock()
	if err := m.usable(); err != nil {
		m.mu.RUnlock()
		return err
	}
	client, ok := m.clients[name]
	if !ok {
		m.mu.RUnlock()
		return errors.New("cache: 数据源不存在")
	}
	timeout := m.cfg.Sources[name].pingTimeout
	m.mu.RUnlock()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return client.Ping(ctx).Err()
}

// Close 幂等关闭所有客户端，由 core 在 RPC 排空和探针关闭后调用。
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		done := m.closeDone
		m.mu.Unlock()
		<-done
		m.mu.RLock()
		defer m.mu.RUnlock()
		return m.closeErr
	}
	m.closed = true
	clients, started := m.clients, m.started
	m.mu.Unlock()
	// SDK 关闭可能等待正在建连的命令；不持有管理器锁，避免与 DialHook 的状态检查互相等待。
	err := closeClients(clients)
	if started {
		klog.Infof("cache: 全部连接池已关闭：sources=[%d]", len(clients))
	}
	m.mu.Lock()
	m.closeErr = err
	close(m.closeDone)
	m.mu.Unlock()
	return err
}

// usable 在持有管理器锁时校验生命周期，不允许自动重建或回退其他实例。
func (m *Manager) usable() error {
	if m.closed {
		return errors.New("cache: 数据源管理器已关闭")
	}
	if !m.started {
		return errors.New("cache: 尚未通过 WithCache 启动数据源")
	}
	return nil
}

// names 返回稳定排序的数据源名称。
func names[V any](items map[string]V) []string {
	result := make([]string, 0, len(items))
	for name := range items {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

// closeClients 关闭全部已创建客户端，保留具名清理错误。
func closeClients(clients map[string]*redis.Client) error {
	var result error
	for _, name := range names(clients) {
		if err := clients[name].Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("cache: 关闭数据源[%s]失败：%w", name, err))
		}
	}
	return result
}
