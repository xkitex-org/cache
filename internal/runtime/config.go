package runtime

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// settings 保存一次启动合并后的具名缓存配置，不读取环境变量。
type settings struct {
	Default string                    `yaml:"default"`
	Sources map[string]sourceSettings `yaml:"sources"`
}

// sourceSettings 描述一个单节点 Redis/Valkey 数据源及其独立连接池。
type sourceSettings struct {
	Type                                                             string `yaml:"type"`
	Address                                                          string `yaml:"address"`
	Username                                                         string `yaml:"username"`
	Password                                                         string `yaml:"password"`
	DB                                                               int    `yaml:"db"`
	PoolSize                                                         int    `yaml:"pool_size"`
	DialTimeout                                                      string `yaml:"dial_timeout"`
	ReadTimeout                                                      string `yaml:"read_timeout"`
	WriteTimeout                                                     string `yaml:"write_timeout"`
	PoolTimeout                                                      string `yaml:"pool_timeout"`
	PingTimeout                                                      string `yaml:"ping_timeout"`
	dialTimeout, readTimeout, writeTimeout, poolTimeout, pingTimeout time.Duration
}

var sourceName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// parseSettings 严格校验整个 cache 节，返回的错误只包含字段名和安全的数据源名称。
func parseSettings(body []byte) (settings, error) {
	var cfg settings
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, errors.New("cache: 配置必须是合法对象，且不能含未知或重复字段")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return cfg, errors.New("cache: 不允许多个 YAML 文档")
	}
	if !sourceName.MatchString(cfg.Default) || len(cfg.Sources) == 0 {
		return cfg, errors.New("cache: 必须配置 default 和至少一个具名 sources")
	}
	if _, ok := cfg.Sources[cfg.Default]; !ok {
		return cfg, errors.New("cache: default 必须指向已配置的数据源")
	}
	for name, source := range cfg.Sources {
		if !sourceName.MatchString(name) {
			return cfg, errors.New("cache: 数据源名称仅允许字母、数字、点、下划线和连字符，并以字母或数字开头")
		}
		if err := source.validate(); err != nil {
			return cfg, fmt.Errorf("cache: 数据源[%s]：%w", name, err)
		}
		cfg.Sources[name] = source
	}
	return cfg, nil
}

// validate 应用固定默认值，拒绝 URL、认证信息嵌入地址及无效连接参数。
func (s *sourceSettings) validate() error {
	if s.Type != "redis" && s.Type != "valkey" {
		return errors.New("type 仅支持 redis 或 valkey")
	}
	host, port, err := net.SplitHostPort(s.Address)
	n, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || host == "" || n < 1 || n > 65535 || strings.ContainsAny(host, "/@?#\r\n\t ") {
		return errors.New("address 必须是明确的 host:port，不允许 URL 或内嵌认证信息")
	}
	if s.DB < 0 {
		return errors.New("db 不能小于 0")
	}
	if s.PoolSize == 0 {
		s.PoolSize = 10
	}
	if s.PoolSize < 1 {
		return errors.New("pool_size 必须大于 0")
	}
	for _, item := range []struct {
		raw      string
		fallback time.Duration
		dest     *time.Duration
	}{
		{s.DialTimeout, 5 * time.Second, &s.dialTimeout}, {s.ReadTimeout, 5 * time.Second, &s.readTimeout},
		{s.WriteTimeout, 5 * time.Second, &s.writeTimeout}, {s.PoolTimeout, 5 * time.Second, &s.poolTimeout},
		{s.PingTimeout, 3 * time.Second, &s.pingTimeout},
	} {
		*item.dest = item.fallback
		if item.raw != "" {
			duration, err := time.ParseDuration(item.raw)
			if err != nil || duration <= 0 {
				return errors.New("连接、读写、连接池和 Ping 超时必须是正的 Go duration")
			}
			*item.dest = duration
		}
	}
	return nil
}
