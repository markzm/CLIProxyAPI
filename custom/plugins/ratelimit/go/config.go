package main

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultTypeName             = "default"
	defaultChannelPerAuth       = 2
	defaultQueuePerAuth         = 100
	defaultMaxQueueSize         = 500
	defaultQueueTimeoutSeconds  = 600
	defaultWindowMinutes        = 10
	defaultAuthSyncIntervalSecs = 10
	defaultMaxTotalWaiters      = 1000
	defaultMaxQueuedBytes       = 256 << 20

	// maxConfigValue bounds every count so products such as auths*per_auth cannot overflow.
	maxConfigValue = 1_000_000
	// maxConfigBytes bounds max_queued_bytes (64 GiB).
	maxConfigBytes = 64 << 30
)

// ClientTypeRule defines the resolved rate limiting and queuing rule for one client type.
// Zero means "not configured" for every numeric field.
type ClientTypeRule struct {
	Channel                 string
	StaticMaxConcurrency    int
	StaticMaxQueueSize      int
	MaxQueueSize            int
	QueuePerAuth            int
	QueueTimeoutSeconds     int
	WindowMinutes           int
	WindowMaxRequests       int
	WindowPerAuth           int
	StaticWindowMaxRequests int
}

// PluginConfig is the validated configuration parsed from plugins.configs.<pluginID>.
type PluginConfig struct {
	ChannelPerAuth          map[string]int
	DefaultChannelPerAuth   int
	ChannelQueuePerAuth     map[string]int
	DefaultQueuePerAuth     int
	ChannelWindowPerAuth    map[string]int
	DefaultWindowPerAuth    int
	AuthSyncIntervalSeconds int
	WindowMinutes           int
	WindowMaxRequests       int
	MaxTotalWaiters         int
	MaxQueuedBytes          int64
	MetricsToken            string
	Types                   map[string]ClientTypeRule
	DefaultRule             ClientTypeRule
}

// rawRule mirrors ClientTypeRule with pointers so explicit values can be told apart from unset ones.
type rawRule struct {
	Channel                 *string `yaml:"channel"`
	StaticMaxConcurrency    *int    `yaml:"static_max_concurrency"`
	StaticMaxQueueSize      *int    `yaml:"static_max_queue_size"`
	MaxQueueSize            *int    `yaml:"max_queue_size"`
	QueuePerAuth            *int    `yaml:"queue_per_auth"`
	QueueTimeoutSeconds     *int    `yaml:"queue_timeout_seconds"`
	WindowMinutes           *int    `yaml:"window_minutes"`
	WindowMaxRequests       *int    `yaml:"window_max_requests"`
	WindowPerAuth           *int    `yaml:"window_per_auth"`
	StaticWindowMaxRequests *int    `yaml:"static_window_max_requests"`
}

type rawConfig struct {
	ChannelPerAuth          map[string]int     `yaml:"channel_per_auth"`
	DefaultChannelPerAuth   *int               `yaml:"default_channel_per_auth"`
	ChannelQueuePerAuth     map[string]int     `yaml:"channel_queue_per_auth"`
	QueuePerAuth            map[string]int     `yaml:"queue_per_auth"`
	DefaultQueuePerAuth     *int               `yaml:"default_queue_per_auth"`
	ChannelWindowPerAuth    map[string]int     `yaml:"channel_window_per_auth"`
	WindowPerAuth           map[string]int     `yaml:"window_per_auth"`
	DefaultWindowPerAuth    *int               `yaml:"default_window_per_auth"`
	AuthSyncIntervalSeconds *int               `yaml:"auth_sync_interval_seconds"`
	WindowMinutes           *int               `yaml:"window_minutes"`
	WindowMaxRequests       *int               `yaml:"window_max_requests"`
	MaxTotalWaiters         *int               `yaml:"max_total_waiters"`
	MaxQueuedBytes          *int64             `yaml:"max_queued_bytes"`
	MetricsToken            *string            `yaml:"metrics_token"`
	Types                   map[string]rawRule `yaml:"types"`
	DefaultRule             *rawRule           `yaml:"default_rule"`
}

// DefaultPluginConfig returns the built-in configuration.
func DefaultPluginConfig() PluginConfig {
	return PluginConfig{
		ChannelPerAuth:          map[string]int{"codex": 5, "claude": 3, "gemini": 10, "xai": 4},
		DefaultChannelPerAuth:   defaultChannelPerAuth,
		ChannelQueuePerAuth:     map[string]int{"codex": 100, "claude": 100, "gemini": 100, "xai": 100},
		DefaultQueuePerAuth:     defaultQueuePerAuth,
		ChannelWindowPerAuth:    map[string]int{},
		AuthSyncIntervalSeconds: defaultAuthSyncIntervalSecs,
		MaxTotalWaiters:         defaultMaxTotalWaiters,
		MaxQueuedBytes:          defaultMaxQueuedBytes,
		Types: map[string]ClientTypeRule{
			"codex":       {Channel: "codex", MaxQueueSize: defaultMaxQueueSize, QueueTimeoutSeconds: defaultQueueTimeoutSeconds},
			"claude-code": {Channel: "claude", MaxQueueSize: defaultMaxQueueSize, QueueTimeoutSeconds: defaultQueueTimeoutSeconds},
			"gemini":      {Channel: "gemini", MaxQueueSize: defaultMaxQueueSize, QueueTimeoutSeconds: defaultQueueTimeoutSeconds},
			"ide-plugin":  {Channel: "codex", MaxQueueSize: defaultMaxQueueSize, QueueTimeoutSeconds: defaultQueueTimeoutSeconds},
			"script-bot": {
				Channel:              defaultTypeName,
				StaticMaxConcurrency: 2,
				StaticMaxQueueSize:   1000,
				MaxQueueSize:         1000,
				QueueTimeoutSeconds:  defaultQueueTimeoutSeconds,
			},
		},
		DefaultRule: ClientTypeRule{
			Channel:             defaultTypeName,
			MaxQueueSize:        defaultMaxQueueSize,
			QueueTimeoutSeconds: defaultQueueTimeoutSeconds,
		},
	}
}

// ParseConfig parses YAML, merges explicit values field-by-field onto the defaults and validates the result.
func ParseConfig(rawYAML []byte) (PluginConfig, error) {
	cfg := DefaultPluginConfig()
	if len(strings.TrimSpace(string(rawYAML))) == 0 {
		return cfg, nil
	}
	var raw rawConfig
	if err := yaml.Unmarshal(rawYAML, &raw); err != nil {
		return cfg, err
	}

	// Explicit user maps are merged key-by-key over the built-in defaults.
	// queue_per_auth/window_per_auth are aliases; the channel_* spelling wins when both set a key.
	cfg.ChannelPerAuth = mergeIntMap(cfg.ChannelPerAuth, raw.ChannelPerAuth)
	cfg.ChannelQueuePerAuth = mergeIntMap(mergeIntMap(cfg.ChannelQueuePerAuth, raw.QueuePerAuth), raw.ChannelQueuePerAuth)
	cfg.ChannelWindowPerAuth = mergeIntMap(mergeIntMap(cfg.ChannelWindowPerAuth, raw.WindowPerAuth), raw.ChannelWindowPerAuth)
	setInt(&cfg.DefaultChannelPerAuth, raw.DefaultChannelPerAuth)
	setInt(&cfg.DefaultQueuePerAuth, raw.DefaultQueuePerAuth)
	setInt(&cfg.DefaultWindowPerAuth, raw.DefaultWindowPerAuth)
	setInt(&cfg.AuthSyncIntervalSeconds, raw.AuthSyncIntervalSeconds)
	setInt(&cfg.WindowMinutes, raw.WindowMinutes)
	setInt(&cfg.WindowMaxRequests, raw.WindowMaxRequests)
	setInt(&cfg.MaxTotalWaiters, raw.MaxTotalWaiters)
	if raw.MaxQueuedBytes != nil {
		cfg.MaxQueuedBytes = *raw.MaxQueuedBytes
	}
	if raw.MetricsToken != nil {
		cfg.MetricsToken = strings.TrimSpace(*raw.MetricsToken)
	}
	if raw.DefaultRule != nil {
		cfg.DefaultRule = raw.DefaultRule.mergeInto(cfg.DefaultRule)
	}
	for name, rule := range raw.Types {
		key := normalizeName(name)
		if key == "" {
			return cfg, fmt.Errorf("types: empty client type name")
		}
		if key == defaultTypeName {
			return cfg, fmt.Errorf("types: %q is reserved, use default_rule", defaultTypeName)
		}
		base, ok := cfg.Types[key]
		if !ok {
			// New client types inherit the default rule before applying their own explicit fields.
			base = cfg.DefaultRule
		}
		cfg.Types[key] = rule.mergeInto(base)
	}

	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (r rawRule) mergeInto(base ClientTypeRule) ClientTypeRule {
	if r.Channel != nil {
		base.Channel = *r.Channel
	}
	setInt(&base.StaticMaxConcurrency, r.StaticMaxConcurrency)
	setInt(&base.StaticMaxQueueSize, r.StaticMaxQueueSize)
	setInt(&base.MaxQueueSize, r.MaxQueueSize)
	setInt(&base.QueuePerAuth, r.QueuePerAuth)
	setInt(&base.QueueTimeoutSeconds, r.QueueTimeoutSeconds)
	setInt(&base.WindowMinutes, r.WindowMinutes)
	setInt(&base.WindowMaxRequests, r.WindowMaxRequests)
	setInt(&base.WindowPerAuth, r.WindowPerAuth)
	setInt(&base.StaticWindowMaxRequests, r.StaticWindowMaxRequests)
	return base
}

func (c *PluginConfig) validate() error {
	checks := map[string]int{
		"default_channel_per_auth":   c.DefaultChannelPerAuth,
		"default_queue_per_auth":     c.DefaultQueuePerAuth,
		"default_window_per_auth":    c.DefaultWindowPerAuth,
		"auth_sync_interval_seconds": c.AuthSyncIntervalSeconds,
		"window_minutes":             c.WindowMinutes,
		"window_max_requests":        c.WindowMaxRequests,
		"max_total_waiters":          c.MaxTotalWaiters,
	}
	for key, value := range checks {
		if err := checkCount(key, value); err != nil {
			return err
		}
	}
	if c.AuthSyncIntervalSeconds == 0 {
		c.AuthSyncIntervalSeconds = defaultAuthSyncIntervalSecs
	}
	if c.MaxTotalWaiters == 0 {
		return fmt.Errorf("max_total_waiters must be positive")
	}
	if c.MaxQueuedBytes <= 0 || c.MaxQueuedBytes > maxConfigBytes {
		return fmt.Errorf("max_queued_bytes must be in (0, %d]", int64(maxConfigBytes))
	}
	for name, values := range map[string]map[string]int{
		"channel_per_auth":        c.ChannelPerAuth,
		"channel_queue_per_auth":  c.ChannelQueuePerAuth,
		"channel_window_per_auth": c.ChannelWindowPerAuth,
	} {
		normalized := make(map[string]int, len(values))
		for channel, value := range values {
			if err := checkCount(name+"."+channel, value); err != nil {
				return err
			}
			normalized[normalizeName(channel)] = value
		}
		switch name {
		case "channel_per_auth":
			c.ChannelPerAuth = normalized
		case "channel_queue_per_auth":
			c.ChannelQueuePerAuth = normalized
		default:
			c.ChannelWindowPerAuth = normalized
		}
	}
	rule, err := validateRule("default_rule", c.DefaultRule)
	if err != nil {
		return err
	}
	c.DefaultRule = rule
	for name, rule := range c.Types {
		validated, errRule := validateRule("types."+name, rule)
		if errRule != nil {
			return errRule
		}
		c.Types[name] = validated
	}
	return nil
}

func validateRule(prefix string, rule ClientTypeRule) (ClientTypeRule, error) {
	for key, value := range map[string]int{
		"static_max_concurrency":     rule.StaticMaxConcurrency,
		"static_max_queue_size":      rule.StaticMaxQueueSize,
		"max_queue_size":             rule.MaxQueueSize,
		"queue_per_auth":             rule.QueuePerAuth,
		"queue_timeout_seconds":      rule.QueueTimeoutSeconds,
		"window_minutes":             rule.WindowMinutes,
		"window_max_requests":        rule.WindowMaxRequests,
		"window_per_auth":            rule.WindowPerAuth,
		"static_window_max_requests": rule.StaticWindowMaxRequests,
	} {
		if err := checkCount(prefix+"."+key, value); err != nil {
			return rule, err
		}
	}
	rule.Channel = normalizeName(rule.Channel)
	if rule.Channel == "" {
		rule.Channel = defaultTypeName
	}
	return rule, nil
}

func checkCount(key string, value int) error {
	if value < 0 || value > maxConfigValue {
		return fmt.Errorf("%s must be in [0, %d], got %d", key, maxConfigValue, value)
	}
	return nil
}

func mergeIntMap(base, override map[string]int) map[string]int {
	out := make(map[string]int, len(base)+len(override))
	for k, v := range base {
		out[normalizeName(k)] = v
	}
	for k, v := range override {
		out[normalizeName(k)] = v
	}
	return out
}

func setInt(dst *int, src *int) {
	if src != nil {
		*dst = *src
	}
}

func normalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// RuleFor returns the rule for a configured client type, or the default rule.
func (c PluginConfig) RuleFor(clientType string) ClientTypeRule {
	if rule, ok := c.Types[clientType]; ok {
		return rule
	}
	return c.DefaultRule
}

// QueuePolicy is the complete effective limit set applied to one queue at once.
type QueuePolicy struct {
	Channel    string
	AuthCount  int
	Capacity   int
	MaxQueue   int
	WindowSize time.Duration
	WindowMax  int
	Timeout    time.Duration
}

// EffectivePolicy resolves a rule into concrete limits for the given number of eligible credentials.
//
// Precedence:
//   - concurrency: static_max_concurrency > channel_per_auth[channel] (or default_channel_per_auth) * accounts
//   - queue: static_max_queue_size > queue_per_auth (type > channel map > default) * accounts, capped by max_queue_size
//   - window max: static_window_max_requests > window_per_auth (type > channel map > default) * accounts
//     > window_max_requests (type > global); 0 disables the window
//   - window duration: type window_minutes > global window_minutes > 10
//
// With zero eligible credentials, per-auth limits are sized as one account so requests still reach
// the host, which reports its own "no credential" error.
func EffectivePolicy(cfg PluginConfig, rule ClientTypeRule, authCount int) QueuePolicy {
	accounts := authCount
	if accounts < 1 {
		accounts = 1
	}
	channel := rule.Channel
	if channel == "" {
		channel = defaultTypeName
	}
	p := QueuePolicy{Channel: channel, AuthCount: authCount}

	if rule.StaticMaxConcurrency > 0 {
		p.Capacity = rule.StaticMaxConcurrency
	} else {
		perAuth := firstPositive(cfg.ChannelPerAuth[channel], cfg.DefaultChannelPerAuth, defaultChannelPerAuth)
		p.Capacity = accounts * perAuth
	}

	if rule.StaticMaxQueueSize > 0 {
		p.MaxQueue = rule.StaticMaxQueueSize
	} else {
		perAuth := firstPositive(rule.QueuePerAuth, cfg.ChannelQueuePerAuth[channel], cfg.DefaultQueuePerAuth)
		if perAuth > 0 {
			p.MaxQueue = accounts * perAuth
		}
		if rule.MaxQueueSize > 0 && (p.MaxQueue == 0 || rule.MaxQueueSize < p.MaxQueue) {
			p.MaxQueue = rule.MaxQueueSize
		}
		if p.MaxQueue == 0 {
			p.MaxQueue = defaultMaxQueueSize
		}
	}

	if rule.StaticWindowMaxRequests > 0 {
		p.WindowMax = rule.StaticWindowMaxRequests
	} else if perAuth := firstPositive(rule.WindowPerAuth, cfg.ChannelWindowPerAuth[channel], cfg.DefaultWindowPerAuth); perAuth > 0 {
		p.WindowMax = accounts * perAuth
	} else {
		p.WindowMax = firstPositive(rule.WindowMaxRequests, cfg.WindowMaxRequests)
	}
	if p.WindowMax > 0 {
		p.WindowSize = time.Duration(firstPositive(rule.WindowMinutes, cfg.WindowMinutes, defaultWindowMinutes)) * time.Minute
	}

	p.Timeout = time.Duration(firstPositive(rule.QueueTimeoutSeconds, cfg.DefaultRule.QueueTimeoutSeconds, defaultQueueTimeoutSeconds)) * time.Second
	return p
}

func firstPositive(values ...int) int {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}
