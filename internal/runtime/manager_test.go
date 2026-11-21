package runtime

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// testManager 创建两个独立 RESP 服务，分别模拟 Valkey 和带认证的 Redis 数据源。
func testManager(t *testing.T) (*Manager, *miniredis.Miniredis, *miniredis.Miniredis) {
	t.Helper()
	primary, session := miniredis.RunT(t), miniredis.RunT(t)
	session.RequireUserAuth("app", "secret-marker")
	m := New()
	body := fmt.Sprintf(`default: primary
sources:
  primary: {type: valkey, address: %q, db: 2, ping_timeout: 100ms}
  session: {type: redis, address: %q, username: app, password: secret-marker, db: 3, ping_timeout: 100ms}
`, primary.Addr(), session.Addr())
	if err := m.ConfigureCache([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m, primary, session
}

// TestIndependentSourcesAndNativeCommands 验证无启动 Ping、默认选择、认证、DB 隔离及原生命令返回值。
func TestIndependentSourcesAndNativeCommands(t *testing.T) {
	m, primary, sessionServer := testManager(t)
	if primary.TotalConnectionCount() != 0 || sessionServer.TotalConnectionCount() != 0 {
		t.Fatal("startup contacted a server without monitor")
	}
	ctx := context.Background()
	h := m.Cache()
	key := h.Key("product:1")
	if key != "product:1" {
		t.Fatalf("default prefix changed existing key: %q", key)
	}
	if err := h.Set(ctx, key, "商品", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	value, err := h.Get(ctx, "product:1").Result()
	if err != nil || value != "商品" {
		t.Fatalf("native result: %q %v", value, err)
	}
	if !primary.DB(2).Exists("product:1") || primary.DB(0).Exists("product:1") {
		t.Fatal("wrong logical DB")
	}
	if err := h.Get(ctx, "missing").Err(); err != redis.Nil {
		t.Fatalf("redis.Nil changed: %v", err)
	}
	second, err := h.WithDataSource("session")
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Set(ctx, "product:1", "会话", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if value, err := m.Cache().Get(ctx, "product:1").Result(); err != nil || value != "商品" {
		t.Fatal("named selection changed default")
	}
	if !sessionServer.DB(3).Exists("product:1") {
		t.Fatal("source routing incorrect")
	}
	if _, err := h.WithDataSource("missing"); err == nil {
		t.Fatal("unknown name fell back")
	}
	if value, err := h.Do(ctx, "GET", "product:1").Text(); err != nil || value != "商品" {
		t.Fatalf("Do: %q %v", value, err)
	}
	primary.FastForward(time.Minute)
	if err := h.Get(ctx, "product:1").Err(); err != redis.Nil {
		t.Fatal("expiration was lost")
	}
}

// TestPipelineAndCancellation 验证普通及 MULTI/EXEC 管道沿用原生命令语义，取消后不会写入。
func TestPipelineAndCancellation(t *testing.T) {
	m, primary, _ := testManager(t)
	h := m.Cache()
	ctx := context.Background()
	_, err := h.Pipelined(ctx, func(p redis.Pipeliner) error { p.Set(ctx, "one", 1, 0); p.Set(ctx, "two", 2, 0); return nil })
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.TxPipelined(ctx, func(p redis.Pipeliner) error { p.Incr(ctx, "one"); p.Incr(ctx, "two"); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if value, err := h.Get(ctx, "one").Int(); err != nil || value != 2 {
		t.Fatalf("transaction pipeline: %d %v", value, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := h.Set(canceled, "canceled", "value", 0).Err(); err == nil {
		t.Fatal("cancellation ignored")
	}
	if primary.DB(2).Exists("canceled") {
		t.Fatal("canceled command wrote data")
	}
}

// TestReadinessRecoveryAndNoLivenessSideEffect 验证逐源 Ping、故障恢复及调用方截止时间。
func TestReadinessRecoveryAndNoLivenessSideEffect(t *testing.T) {
	m, primary, _ := testManager(t)
	checks := m.ReadinessChecks()
	if len(checks) != 2 || checks["cache.primary"] == nil || checks["cache.session"] == nil {
		t.Fatal("named readiness missing")
	}
	for name, check := range checks {
		if err := check(context.Background()); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	primary.SetError("LOADING simulated failure")
	if err := checks["cache.primary"](context.Background()); err == nil {
		t.Fatal("failed dependency was healthy")
	}
	if err := checks["cache.session"](context.Background()); err != nil {
		t.Fatal("failure affected other source")
	}
	primary.SetError("")
	if err := checks["cache.primary"](context.Background()); err != nil {
		t.Fatal("dependency did not recover")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := checks["cache.primary"](ctx); err == nil {
		t.Fatal("healthcheck ignored canceled context")
	}
}

// TestCloseRejectsCommandsAndPipelines 验证已取得句柄及未执行管道在关闭后都拒绝写入。
func TestCloseRejectsCommandsAndPipelines(t *testing.T) {
	m, primary, _ := testManager(t)
	h := m.Cache()
	ctx := context.Background()
	if err := h.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	pipeline := h.Pipeline()
	cmd := pipeline.Set(ctx, "late", "value", 0)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.Exec(ctx); err == nil || cmd.Err() == nil {
		t.Fatal("closed pipeline reported success")
	}
	if err := h.Get(ctx, "late").Err(); err == nil {
		t.Fatal("old handle remained usable")
	}
	if err := m.Cache().Get(ctx, "late").Err(); err == nil {
		t.Fatal("closed manager returned live default")
	}
	if primary.DB(2).Exists("late") {
		t.Fatal("closed pipeline wrote data")
	}
}

// TestUninitializedHandleDoesNotDial 验证未启用时返回原生错误命令，不进行默认地址连接。
func TestUninitializedHandleDoesNotDial(t *testing.T) {
	m := New()
	if m.clients != nil || len(m.ReadinessChecks()) != 0 {
		t.Fatal("constructor initialized resources")
	}
	h := m.Cache()
	ctx := context.Background()
	if err := h.Get(ctx, "key").Err(); err == nil {
		t.Fatal("uninitialized get succeeded")
	}
	if err := h.Set(ctx, "key", "value", 0).Err(); err == nil {
		t.Fatal("uninitialized set succeeded")
	}
	if err := h.Do(ctx, "PING").Err(); err == nil {
		t.Fatal("uninitialized Do succeeded")
	}
	p := h.TxPipeline()
	cmd := p.Set(ctx, "key", "value", 0)
	if _, err := p.Exec(ctx); err == nil || cmd.Err() == nil {
		t.Fatal("uninitialized pipeline succeeded")
	}
	if _, err := h.WithDataSource("primary"); err == nil {
		t.Fatal("source resolved before start")
	}
}

// TestConcurrentCallsAndSources 验证请求共享原生客户端时数据源选择不会串线。
func TestConcurrentCallsAndSources(t *testing.T) {
	m, _, _ := testManager(t)
	primary := m.Cache()
	second, err := primary.WithDataSource("session")
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			ctx := context.Background()
			if err := primary.Incr(ctx, "count").Err(); err != nil {
				t.Error(err)
			}
			if err := second.Incr(ctx, "count").Err(); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	for _, handle := range []*Handle{primary, second} {
		if count, err := handle.Get(context.Background(), "count").Int(); err != nil || count != 16 {
			t.Fatalf("count: %d %v", count, err)
		}
	}
}

// TestConcurrentCloseAndInitialization 验证命令首次建连与并发关闭不会产生锁反转或重复释放。
func TestConcurrentCloseAndInitialization(t *testing.T) {
	m, _, _ := testManager(t)
	h := m.Cache()
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() { defer group.Done(); _ = h.Ping(context.Background()).Err() }()
	}
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := m.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	done := make(chan struct{})
	go func() { group.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("close and connection initialization deadlocked")
	}
}

// TestHealthDeadlineOnSilentServer 验证服务端不回复时 monitor 的更短超时仍会生效。
func TestHealthDeadlineOnSilentServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	m := New()
	body := fmt.Sprintf("default: primary\nsources:\n  primary: {type: redis, address: %q, ping_timeout: 2s}\n", listener.Addr().String())
	if err := m.ConfigureCache([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = m.ReadinessChecks()["cache.primary"](ctx)
	if err == nil || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("health deadline ignored: %v", err)
	}
	select {
	case conn := <-accepted:
		_ = conn.Close()
	case <-time.After(time.Second):
		t.Fatal("test server never accepted connection")
	}
}

// TestStrictSingleShotConfiguration 验证配置只读一次、明确默认值和错误信息不会泄露认证配置。
func TestStrictSingleShotConfiguration(t *testing.T) {
	valid := "default: primary\nsources:\n  primary: {type: valkey, address: 'localhost:6379', password: secret-marker}\n"
	for _, body := range []string{"", "[]", "{}", valid + "unknown: secret-marker", valid + "default: primary", valid + "---\n{}",
		strings.Replace(valid, "valkey", "memcached", 1), strings.Replace(valid, "localhost:6379", "redis://user:secret-marker@host:6379", 1),
		strings.Replace(valid, "password: secret-marker", "db: -1", 1), strings.Replace(valid, "password: secret-marker", "pool_size: -1", 1),
		strings.Replace(valid, "password: secret-marker", "ping_timeout: wrong", 1), strings.Replace(valid, "default: primary", "default: missing", 1)} {
		if _, err := parseSettings([]byte(body)); err == nil || strings.Contains(err.Error(), "secret-marker") {
			t.Fatalf("invalid config accepted or leaked: %v", err)
		}
	}
	m := New()
	if err := m.ConfigureCache([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	if err := m.ConfigureCache([]byte(valid)); err == nil {
		t.Fatal("config reloaded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.Start(ctx); err == nil {
		t.Fatal("canceled start accepted")
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Start(context.Background()); err == nil {
		t.Fatal("duplicate start accepted")
	}
}

func TestConfiguredKeyPrefixIsSharedAcrossSources(t *testing.T) {
	redisServer := miniredis.RunT(t)
	m := New()
	defer m.Close()
	body := fmt.Sprintf(`default: primary
key_prefix: "test-app:"
sources:
  primary: {type: redis, address: %q}
  secondary: {type: valkey, address: %q, db: 1}
`, redisServer.Addr(), redisServer.Addr())
	if err := m.ConfigureCache([]byte(body)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	primary := m.Cache()
	secondary, err := primary.WithDataSource("secondary")
	if err != nil {
		t.Fatal(err)
	}
	for db, handle := range []*Handle{primary, secondary} {
		key := handle.Key("item:{42}")
		_, err := handle.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, key, "value", time.Minute)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if got, err := handle.Eval(ctx, "return redis.call('GET', KEYS[1])", []string{key}).Text(); err != nil || got != "value" {
			t.Fatalf("could not read prefixed key in DB %d: %q, %v", db, got, err)
		}
		if !redisServer.DB(db).Exists("test-app:item:{42}") || redisServer.DB(db).Exists("item:{42}") {
			t.Fatalf("unexpected keys in DB %d: %v", db, redisServer.DB(db).Keys())
		}
	}
}
