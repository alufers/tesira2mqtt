// Package config loads the bridge's YAML configuration.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the whole configuration file.
type Config struct {
	LogLevel string   `yaml:"log_level"`
	MQTT     MQTT     `yaml:"mqtt"`
	Systems  []System `yaml:"systems"`
}

// MQTT describes the broker connection shared by every system.
type MQTT struct {
	Broker         string        `yaml:"broker"`
	ClientID       string        `yaml:"client_id"`
	Username       string        `yaml:"username"`
	Password       string        `yaml:"password"`
	Prefix         string        `yaml:"prefix"`
	QoS            byte          `yaml:"qos"`
	Retain         bool          `yaml:"retain"`
	KeepAlive      time.Duration `yaml:"keep_alive"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	InsecureTLS    bool          `yaml:"insecure_tls"`
}

// System is one Tesira device to bridge.
type System struct {
	// Name is the second topic segment. Empty means "use the device's own
	// hostname", read from the DSP once connected.
	Name string `yaml:"name"`

	Transport string `yaml:"transport"` // ssh | telnet
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`

	// HostKeyVerification enables SSH host key checking against KnownHostsFile.
	HostKeyVerification bool   `yaml:"host_key_verification"`
	KnownHostsFile      string `yaml:"known_hosts_file"`

	ConnectTimeout      time.Duration `yaml:"connect_timeout"`
	CommandTimeout      time.Duration `yaml:"command_timeout"`
	ReconnectInterval   time.Duration `yaml:"reconnect_interval"`
	ResubscribeInterval time.Duration `yaml:"resubscribe_interval"`

	// BlockTypes, when non-empty, limits bridging to these Tesira block types.
	BlockTypes []string `yaml:"block_types"`
	// SkipBlocks lists instance tags to ignore entirely.
	SkipBlocks []string `yaml:"skip_blocks"`
}

// Transport kinds.
const (
	TransportSSH    = "ssh"
	TransportTelnet = "telnet"
)

// Load reads, expands and validates a configuration file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	// Environment expansion keeps broker and device credentials out of the
	// file when it is baked into an image.
	expanded := os.ExpandEnv(string(raw))

	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.MQTT.ClientID == "" {
		c.MQTT.ClientID = "tesira2mqtt"
	}
	if c.MQTT.Prefix == "" {
		c.MQTT.Prefix = "tesira"
	}
	if c.MQTT.KeepAlive == 0 {
		c.MQTT.KeepAlive = 30 * time.Second
	}
	if c.MQTT.ConnectTimeout == 0 {
		c.MQTT.ConnectTimeout = 10 * time.Second
	}

	for i := range c.Systems {
		s := &c.Systems[i]
		if s.Transport == "" {
			s.Transport = TransportSSH
		}
		if s.Port == 0 {
			switch s.Transport {
			case TransportTelnet:
				s.Port = 23
			default:
				s.Port = 22
			}
		}
		if s.Username == "" {
			s.Username = "default"
		}
		if s.ConnectTimeout == 0 {
			s.ConnectTimeout = 10 * time.Second
		}
		if s.CommandTimeout == 0 {
			s.CommandTimeout = 5 * time.Second
		}
		if s.ReconnectInterval == 0 {
			s.ReconnectInterval = 5 * time.Second
		}
		if s.ResubscribeInterval == 0 {
			s.ResubscribeInterval = 60 * time.Second
		}
	}
}

// Validate reports configuration that cannot work, so startup fails loudly
// rather than after a confusing runtime error.
func (c *Config) Validate() error {
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log_level %q must be debug, info, warn or error", c.LogLevel)
	}

	if c.MQTT.Broker == "" {
		return fmt.Errorf("mqtt.broker is required")
	}
	if u, err := url.Parse(c.MQTT.Broker); err != nil {
		return fmt.Errorf("mqtt.broker %q: %w", c.MQTT.Broker, err)
	} else if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("mqtt.broker %q must look like tcp://host:1883", c.MQTT.Broker)
	}
	if c.MQTT.QoS > 2 {
		return fmt.Errorf("mqtt.qos %d must be 0, 1 or 2", c.MQTT.QoS)
	}
	if strings.ContainsAny(c.MQTT.Prefix, "+#") {
		return fmt.Errorf("mqtt.prefix %q must not contain wildcards", c.MQTT.Prefix)
	}

	if len(c.Systems) == 0 {
		return fmt.Errorf("at least one system is required")
	}

	seen := map[string]bool{}
	for i, s := range c.Systems {
		where := fmt.Sprintf("systems[%d]", i)
		if s.Host == "" {
			return fmt.Errorf("%s: host is required", where)
		}
		switch s.Transport {
		case TransportSSH, TransportTelnet:
		default:
			return fmt.Errorf("%s: transport %q must be ssh or telnet", where, s.Transport)
		}
		if s.Port < 1 || s.Port > 65535 {
			return fmt.Errorf("%s: port %d is out of range", where, s.Port)
		}
		// Names are optional; duplicates would collide on the topic tree, and
		// hostname-derived names are checked again at connect time.
		key := s.Name
		if key == "" {
			key = strings.ToLower(s.Host)
		}
		if seen[key] {
			return fmt.Errorf("%s: duplicate system %q", where, key)
		}
		seen[key] = true
	}
	return nil
}
