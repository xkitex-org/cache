// Package cache 提供 Redis/Valkey 具名数据源与逐源就绪检查。
// 引入包及调用 New 不读取配置、连接服务器或启动检查。
package cache

import (
	"context"

	"go.chaoxizhongzhi.com/xkitex/cache/internal/runtime"
)

// Controller 是供 core 显式管理的接入协议，配置内容仅为 cache 节。
type Controller interface {
	ConfigureCache([]byte) error
	Start(context.Context) error
	ReadinessChecks() map[string]func(context.Context) error
	Cache() *Handle
	Close() error
}

// Handle 提供 go-redis 常规命令、管道和具名数据源选择，连接池由 core 统一关闭。
type Handle = runtime.Handle

// New 创建没有资源副作用的独立缓存管理器。
func New() Controller { return runtime.New() }
