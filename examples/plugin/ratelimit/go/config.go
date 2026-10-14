package main

import (
	"gopkg.in/yaml.v3"
)

// ClientTypeRule defines rate limiting and queuing rules for a specific client type.
type ClientTypeRule struct {
	Channel                 string `yaml:"channel" json:"channel"`
	StaticMaxConcurrency    int    `yaml:"static_max_concurrency" json:"static_max_concurrency"`
	StaticMaxQueueSize      int    `yaml:"static_max_queue_size" json:"static_max_queue_size"`
	MaxQueueSize            int    `yaml:"max_queue_size" json:"max_queue_size"`
	QueuePerAuth            int    `yaml:"queue_per_auth" json:"queue_per_auth"`
	QueueTimeoutSeconds     int    `yaml:"queue_timeout_seconds" json:"queue_timeout_seconds"`
	WindowMinutes           int    `yaml:"window_minutes" json:"window_minutes"`
	WindowMaxRequests       int    `yaml:"window_max_requests" json:"window_max_requests"`
	WindowPerAuth           int    `yaml:"window_per_auth" json:"window_per_auth"`
	StaticWindowMaxRequests int    `yaml:"static_window_max_requests" json:"static_window_max_requests"`
}

// PluginConfig defines the configuration parsed from plugins.configs.<pluginID>.
type PluginConfig struct {
	Enabled                 bool                      `yaml:"enabled" json:"enabled"`
	ChannelPerAuth          map[string]int            `yaml:"channel_per_auth" json:"channel_per_auth"`
	DefaultChannelPerAuth   int                       `yaml:"default_channel_per_auth" json:"default_channel_per_auth"`
	ChannelQueuePerAuth     map[string]int            `yaml:"channel_queue_per_auth" json:"channel_queue_per_auth"`
	QueuePerAuth            map[string]int            `yaml:"queue_per_auth" json:"queue_per_auth"`
	DefaultQueuePerAuth     int                       `yaml:"default_queue_per_auth" json:"default_queue_per_auth"`
	ChannelWindowPerAuth    map[string]int            `yaml:"channel_window_per_auth" json:"channel_window_per_auth"`
	WindowPerAuth           map[string]int            `yaml:"window_per_auth" json:"window_per_auth"`
	DefaultWindowPerAuth    int                       `yaml:"default_window_per_auth" json:"default_window_per_auth"`
	AuthSyncIntervalSeconds int                       `yaml:"auth_sync_interval_seconds" json:"auth_sync_interval_seconds"`
	WindowMinutes           int                       `yaml:"window_minutes" json:"window_minutes"`
	WindowMaxRequests       int                       `yaml:"window_max_requests" json:"window_max_requests"`
	Types                   map[string]ClientTypeRule `yaml:"types" json:"types"`
	DefaultRule             ClientTypeRule            `yaml:"default_rule" json:"default_rule"`
}

// DefaultPluginConfig returns the default fallback configuration.
func DefaultPluginConfig() PluginConfig {
	return PluginConfig{
		Enabled: true,
		ChannelPerAuth: map[string]int{
			"codex":  5,
			"claude": 3,
			"gemini": 10,
			"xai":    4,
		},
		DefaultChannelPerAuth: 2,
		ChannelQueuePerAuth: map[string]int{
			"codex":  100,
			"claude": 100,
			"gemini": 100,
			"xai":    100,
		},
		DefaultQueuePerAuth: 100,
		ChannelWindowPerAuth: map[string]int{
			"codex":  0,
			"claude": 0,
			"gemini": 0,
			"xai":    0,
		},
		DefaultWindowPerAuth:    0,
		AuthSyncIntervalSeconds: 10,
		WindowMinutes:           10,
		WindowMaxRequests:       0,
		Types: map[string]ClientTypeRule{
			"codex": {
				Channel:             "codex",
				MaxQueueSize:        500,
				QueueTimeoutSeconds: 600,
				WindowMinutes:       10,
				WindowMaxRequests:   0,
			},
			"claude-code": {
				Channel:             "claude",
				MaxQueueSize:        500,
				QueueTimeoutSeconds: 600,
				WindowMinutes:       10,
				WindowMaxRequests:   0,
			},
			"gemini": {
				Channel:             "gemini",
				MaxQueueSize:        500,
				QueueTimeoutSeconds: 600,
				WindowMinutes:       10,
				WindowMaxRequests:   0,
			},
			"ide-plugin": {
				Channel:             "codex",
				MaxQueueSize:        500,
				QueueTimeoutSeconds: 600,
				WindowMinutes:       10,
				WindowMaxRequests:   0,
			},
			"script-bot": {
				Channel:              "default",
				StaticMaxConcurrency: 2,
				StaticMaxQueueSize:   1000,
				MaxQueueSize:         1000,
				QueueTimeoutSeconds:  600,
				WindowMinutes:        10,
				WindowMaxRequests:    0,
			},
		},
		DefaultRule: ClientTypeRule{
			Channel:             "default",
			MaxQueueSize:        500,
			QueueTimeoutSeconds: 600,
			WindowMinutes:       10,
			WindowMaxRequests:   0,
		},
	}
}

// ParseConfig parses YAML configuration and merges with defaults.
func ParseConfig(rawYAML []byte) (PluginConfig, error) {
	cfg := DefaultPluginConfig()
	if len(rawYAML) == 0 {
		return cfg, nil
	}
	if err := yaml.Unmarshal(rawYAML, &cfg); err != nil {
		return cfg, err
	}
	if cfg.DefaultChannelPerAuth <= 0 {
		cfg.DefaultChannelPerAuth = 2
	}
	if cfg.DefaultQueuePerAuth <= 0 {
		cfg.DefaultQueuePerAuth = 100
	}
	if cfg.ChannelQueuePerAuth == nil && cfg.QueuePerAuth != nil {
		cfg.ChannelQueuePerAuth = cfg.QueuePerAuth
	}
	if cfg.ChannelWindowPerAuth == nil && cfg.WindowPerAuth != nil {
		cfg.ChannelWindowPerAuth = cfg.WindowPerAuth
	}
	if cfg.AuthSyncIntervalSeconds <= 0 {
		cfg.AuthSyncIntervalSeconds = 10
	}
	if cfg.WindowMinutes <= 0 {
		cfg.WindowMinutes = 10
	}
	if cfg.DefaultRule.MaxQueueSize <= 0 {
		cfg.DefaultRule.MaxQueueSize = 500
	}
	if cfg.DefaultRule.QueueTimeoutSeconds <= 0 {
		cfg.DefaultRule.QueueTimeoutSeconds = 600
	}
	if cfg.DefaultRule.WindowMinutes <= 0 {
		cfg.DefaultRule.WindowMinutes = cfg.WindowMinutes
	}
	if cfg.DefaultRule.WindowMaxRequests <= 0 && cfg.WindowMaxRequests > 0 {
		cfg.DefaultRule.WindowMaxRequests = cfg.WindowMaxRequests
	}
	return cfg, nil
}
