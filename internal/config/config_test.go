package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alufers/tesira2mqtt/internal/config"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadAppliesDefaults(t *testing.T) {
	path := write(t, `
mqtt:
  broker: tcp://broker:1883
systems:
  - host: 10.0.0.1
  - host: 10.0.0.2
    transport: telnet
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.LogLevel != "info" {
		t.Errorf("log_level = %q", cfg.LogLevel)
	}
	if cfg.MQTT.Prefix != "tesira" {
		t.Errorf("prefix = %q", cfg.MQTT.Prefix)
	}
	if cfg.MQTT.ClientID != "tesira2mqtt" {
		t.Errorf("client_id = %q", cfg.MQTT.ClientID)
	}

	ssh := cfg.Systems[0]
	if ssh.Transport != config.TransportSSH || ssh.Port != 22 {
		t.Errorf("ssh defaults = %q port %d", ssh.Transport, ssh.Port)
	}
	if ssh.Username != "default" {
		t.Errorf("username = %q, want the Tesira default account", ssh.Username)
	}
	if ssh.CommandTimeout != 5*time.Second || ssh.ResubscribeInterval != 60*time.Second {
		t.Errorf("timeouts = %v / %v", ssh.CommandTimeout, ssh.ResubscribeInterval)
	}

	if telnet := cfg.Systems[1]; telnet.Port != 23 {
		t.Errorf("telnet port = %d, want 23", telnet.Port)
	}
}

func TestLoadParsesDurationsAndEnv(t *testing.T) {
	t.Setenv("TEST_MQTT_PASSWORD", "s3cret")
	path := write(t, `
mqtt:
  broker: tcp://broker:1883
  password: ${TEST_MQTT_PASSWORD}
systems:
  - host: 10.0.0.1
    command_timeout: 750ms
    reconnect_interval: 2m
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MQTT.Password != "s3cret" {
		t.Errorf("password = %q, want the expanded environment value", cfg.MQTT.Password)
	}
	if cfg.Systems[0].CommandTimeout != 750*time.Millisecond {
		t.Errorf("command_timeout = %v", cfg.Systems[0].CommandTimeout)
	}
	if cfg.Systems[0].ReconnectInterval != 2*time.Minute {
		t.Errorf("reconnect_interval = %v", cfg.Systems[0].ReconnectInterval)
	}
}

func TestLoadRejectsBadConfig(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"no broker", "systems:\n  - host: 1.2.3.4\n", "mqtt.broker is required"},
		{"no systems", "mqtt:\n  broker: tcp://b:1883\n", "at least one system"},
		{
			"bad broker url",
			"mqtt:\n  broker: broker\nsystems:\n  - host: 1.2.3.4\n",
			"must look like",
		},
		{
			"unknown transport",
			"mqtt:\n  broker: tcp://b:1883\nsystems:\n  - host: 1.2.3.4\n    transport: serial\n",
			"must be ssh or telnet",
		},
		{
			"missing host",
			"mqtt:\n  broker: tcp://b:1883\nsystems:\n  - name: x\n",
			"host is required",
		},
		{
			"duplicate system",
			"mqtt:\n  broker: tcp://b:1883\nsystems:\n  - name: a\n    host: 1.1.1.1\n  - name: a\n    host: 2.2.2.2\n",
			"duplicate system",
		},
		{
			"wildcard prefix",
			"mqtt:\n  broker: tcp://b:1883\n  prefix: a/#\nsystems:\n  - host: 1.2.3.4\n",
			"wildcards",
		},
		{
			"bad log level",
			"log_level: loud\nmqtt:\n  broker: tcp://b:1883\nsystems:\n  - host: 1.2.3.4\n",
			"log_level",
		},
		{
			"unknown field",
			"mqtt:\n  broker: tcp://b:1883\n  brokerr: x\nsystems:\n  - host: 1.2.3.4\n",
			"field brokerr",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Load(write(t, tc.body))
			if err == nil {
				t.Fatalf("expected an error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestExampleConfigIsValid(t *testing.T) {
	cfg, err := config.Load(filepath.Join("..", "..", "config.example.yaml"))
	if err != nil {
		t.Fatalf("the shipped example config does not load: %v", err)
	}
	if len(cfg.Systems) == 0 {
		t.Error("the example config defines no systems")
	}
}
