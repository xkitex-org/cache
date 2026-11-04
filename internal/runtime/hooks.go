package runtime

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/redis/go-redis/v9"
)

// lifecycleHook 在 SDK 执行前拒绝未启动、已关闭及取消的调用，不记录业务键值或命令参数。
type lifecycleHook struct{ manager *Manager }

// check 只检查状态，返回前释放读锁，避免 SDK 建连内部命令再次获取同一把锁。
func (h lifecycleHook) check(ctx context.Context) error {
	if h.manager == nil {
		return errUnavailable
	}
	if ctx == nil {
		return errors.New("cache: context 不能为空")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	h.manager.mu.RLock()
	defer h.manager.mu.RUnlock()
	return h.manager.usable()
}

// DialHook 对真正的连接建立再次校验生命周期，不允许缺省地址回退。
func (h lifecycleHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if err := h.check(ctx); err != nil {
			return nil, err
		}
		return next(ctx, network, address)
	}
}

// ProcessHook 保留 SDK 命令返回类型，并阻止停止后继续执行命令。
func (h lifecycleHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if err := h.check(ctx); err != nil {
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

// ProcessPipelineHook 同时设置管道总体错误及逐命令错误，避免未执行的命令看起来成功。
func (h lifecycleHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if err := h.check(ctx); err != nil {
			for _, cmd := range cmds {
				cmd.SetErr(err)
			}
			return err
		}
		return next(ctx, cmds)
	}
}

var sdkLoggerOnce sync.Once

// installSDKLogger 只在显式启动时安装 SDK 的进程级日志桥接，应在业务协程启动前执行。
func installSDKLogger() { sdkLoggerOnce.Do(func() { redis.SetLogger(sdkLogger{}) }) }

// sdkLogger 把 SDK 诊断转入 Kitex，不回显可能含认证信息、键值及网络响应的原始参数。
type sdkLogger struct{}

// Printf 使用固定安全诊断，具体命令错误仍通过 SDK 返回值交给业务处理。
func (sdkLogger) Printf(ctx context.Context, _ string, _ ...interface{}) {
	if ctx == nil {
		ctx = context.Background()
	}
	klog.CtxWarnf(ctx, "cache: go-redis 内部诊断，命令错误已返回调用方")
}
