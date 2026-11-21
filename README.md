# cache：Redis / Valkey 多数据源

独立模块 `go.chaoxizhongzhi.com/xkitex/cache`，基于 go-redis v9.22.0。当前正式版本为 `v0.0.2`。每个具名数据源对应一个单节点和独立连接池，`type` 支持 `redis`、`valkey`，均通过兼容的 Redis 协议连接。

## 显式接入

```go
import (
    "go.chaoxizhongzhi.com/xkitex/core"
    "go.chaoxizhongzhi.com/xkitex/cache"
    "go.chaoxizhongzhi.com/xkitex/monitor"
)

err := x.Server(
    x.WithCache(cache.New()),
    x.WithMonitor(monitor.New()),
    x.WithService(productservice.RegisterService, productHandler),
)
```

`New` 和选项构造不读取配置、连接服务器或启动检查。core 启动时将合并后的 `cache` 节单次传入。未传 `WithCache` 时忽略该配置节，不创建缓存连接池。没有环境变量开关或密码环境变量覆盖。

core 的无参数入口绑定当前进程唯一的一套应用缓存。业务在应用启动后取得命令句柄；不要在包级变量或启动前缓存句柄。管理器退出后解除绑定，重复绑定不会覆盖运行中的应用。

## 配置

配置仍来自 Consul 固定入口与 imports，application.yaml 字段优先，缓存模块不再次访问 Consul、不建立配置监听。

```yaml
cache:
  default: primary
  # key_prefix: "myapp:"  # 可省略，默认空字符串
  sources:
    primary:
      type: valkey
      address: valkey:6379
      username: default
      password: "管理员维护的密码"
      db: 0
      pool_size: 10
      dial_timeout: 5s
      read_timeout: 5s
      write_timeout: 5s
      pool_timeout: 5s
      ping_timeout: 3s
    session:
      type: redis
      address: redis:6379
      password: "管理员维护的密码"
      db: 1
```

`default` 必须明确指向已配置名称，不根据顺序推断。address 必须为 host:port，拒绝 URL 和内嵌认证信息，不回退 localhost。db 缺省为 0，pool_size 缺省为 10，各超时默认值与示例一致；认证字段可以为空。未知字段、重复字段、多文档、负数库编号和无效超时均阻止启动。

使用 RESP2，关闭 SDK 的 CLIENT SETINFO 和维护通知协商，连接池不预建空闲连接。普通命令关闭自动重试，避免框架自动重复非幂等写入。数据源之间没有自动路由或主从切换；type 表示配置选择，不通过 INFO 校验服务端品牌，具体命令支持范围取决于服务端。

## 业务调用

直接沿用 go-redis 命令、返回类型及管道接口，无须重新封装一套命令：

```go
err := x.Cache().Set(ctx, "product:1", "商品", time.Minute).Err()
value, err := x.Cache().Get(ctx, "product:1").Result()

session, err := x.Cache().WithDataSource("session")
if err != nil {
    return err
}
err = session.Set(ctx, "session:token", "会话", time.Hour).Err()
```

`cache.key_prefix` 是所有具名数据源共用的业务键前缀，默认空字符串。`Handle.Key(key)` 直接拼接前缀，所需分隔符应写入配置，例如使用 `myapp:`。设置后应在读写、删除、管道及 Lua 的 `KEYS` 中统一用 `Key()` 构造键。go-redis 命令接收的是最终键名，已有键不会迁移。

```go
cache := x.Cache()
key := cache.Key("product:1")
err := cache.Set(ctx, key, "商品", time.Minute).Err()
// Lua 使用 []string{key} 传入 KEYS。
```

命令明确接收业务 ctx。未知名称返回错误，不修改默认数据源，也不回退其他数据源。缺失键保持 SDK 的 `redis.Nil`：

```go
value, err := x.Cache().Get(ctx, key).Result()
if errors.Is(err, redis.Nil) {
    // 业务处理缓存未命中。
}
```

普通管道与原生事务管道均可使用：

```go
_, err := x.Cache().Pipelined(ctx, func(p redis.Pipeliner) error {
    p.Set(ctx, "product:1", "商品", time.Minute)
    p.Expire(ctx, "category:1", time.Minute)
    return nil
})

_, err = x.Cache().TxPipelined(ctx, func(p redis.Pipeliner) error {
    p.Incr(ctx, "counter:product")
    p.Incr(ctx, "counter:category")
    return nil
})
```

`TxPipelined` 使用当前缓存数据源的 MULTI/EXEC，语义由 Redis 协议决定；SQL ctx 事务不会把缓存操作纳入同一个数据库事务。额外命令可以通过 `Do(ctx, ...)` 执行。句柄公开 `redis.Cmdable` 常规命令接口，不提供连接池 Close、Options、AddHook 管理方法。

## 健康检查与资源释放

同时接入 monitor 时，core 自动添加 `cache.primary`、`cache.session` 等 readiness Ping。所有缓存和数据库检查通过才就绪；任何依赖失败及恢复继续走现有 monitor、core 生命周期和 Consul 可用性更新逻辑。缓存检查不加入 liveness。

SDK 开启 ContextTimeoutEnabled；Ping 同时受 monitor 更短的 timeout 和数据源 ping_timeout 限制。没有 monitor 时不执行 Ping 或启动周期检查，连接池句柄建立后沿用直接注册可用的约定。数据源暂不可达时，接 monitor 的应用保留进程等待恢复。

core 在 RPC 排空后先停止 monitor，再关闭缓存连接池；同时启用 database 时按启动逆序释放缓存和数据库。配置失败、注册失败及运行失败也清理资源。已关闭句柄和关闭前创建、尚未执行的管道均报告错误，管道逐命令错误同步设置，避免未执行的写入被误判为成功。

所有框架诊断走 Kitex klog，不记录认证信息、缓存键值或命令参数。go-redis 只提供进程级 SDK logger，模块在显式 Start 时安装一次安全桥接，应在业务缓存协程启动前完成；仅引入包或 New 不安装 logger。

## 验证

进入 cache 后可运行独立测试：

```sh
GOWORK=off go test -race -timeout 60s ./...
```

整个未发布工作稿的验证仍复用相邻 database 模块中的脚本：

```sh
python3 ../database/scripts/test-local.py
```

脚本在临时目录生成模块代理并测试 database、cache、core、monitor，不添加 go.work、replace 或上层 go.mod，也不发布源码。

测试用 miniredis v2.38.0 提供本机 RESP 服务，覆盖默认与具名数据源、认证及逻辑库隔离、TTL、redis.Nil、普通与事务管道、取消与超时、故障恢复和并发关闭；core 测试覆盖配置隔离、自动检查挂载及 RPC/monitor/连接池释放顺序。协议模拟服务测试不等同于真实 Redis/Valkey 的版本兼容验证。

根包只有 New、接入协议和必要句柄类型，配置、连接、日志及生命周期实现均位于 internal/runtime；运行时不依赖 core、monitor 或 database 模块。
