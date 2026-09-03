package tesira2mqtt

import (
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Tesira2MQTTConfig struct {
	TesiraSSH SSHConfig  `yaml:"tesira_ssh"`
	MQTT      MQTTConfig `yaml:"mqtt"`
}

// SSHConfig describes an SSH connection to a Tesira device.
type SSHConfig struct {
	Addr         string  `yaml:"addr"`
	Username     string  `yaml:"username"`
	Password     string  `yaml:"password"`
	PasswordFile *string `yaml:"password_file"`
	// If nil, the host key will not be verified.
	KnownHostsFile *string       `yaml:"known_hosts_file"`
	Timeout        time.Duration `yaml:"timeout"`
}

type MQTTConfig struct {
	Broker       string  `yaml:"broker"`
	Username     string  `yaml:"username"`
	Password     string  `yaml:"password"`
	PasswordFile *string `yaml:"password_file"`
	Prefix       string  `yaml:"prefix"`
}

var config *Tesira2MQTTConfig

func loadSecret(target *string, file *string) {

	if *target == "" && file != nil && *file != "" {
		data, err := os.ReadFile(*file)
		if err != nil {
			log.Printf("warning: failed to read secret from file %s: %v", file, err)
			return
		}
		*target = strings.TrimSpace(string(data))
	}
}

func validateConfig(cfg *Tesira2MQTTConfig, path string) {

	loadSecret(&cfg.TesiraSSH.Password, cfg.TesiraSSH.PasswordFile)
	loadSecret(&cfg.MQTT.Password, cfg.MQTT.PasswordFile)
	if cfg.TesiraSSH.Addr == "" {
		log.Fatalf("tesira_ssh.addr required in config %s", path)
	}
	if cfg.TesiraSSH.Username == "" {
		log.Fatalf("tesira_ssh.username required in config %s", path)
	}

	if cfg.MQTT.Broker == "" {
		log.Fatalf("mqtt.broker required in config %s", path)
	}
	if cfg.MQTT.Username == "" {
		log.Fatalf("mqtt.username required in config %s", path)
	}
	if cfg.MQTT.Password == "" {
		log.Fatalf("mqtt.password required in config %s", path)
	}

	if cfg.MQTT.Prefix == "" {
		cfg.MQTT.Prefix = "tesira2mqtt/"
	}

	if cfg.TesiraSSH.Timeout == 0 {
		cfg.TesiraSSH.Timeout = 25 * time.Second
	}
}

func MustLoadConfig() {

	candidates := []string{
		os.Getenv("CONFIG_PATH"),
		"tesira2mqtt.yaml",
		"./tesira2mqtt.yaml",
		"/etc/tesira2mqtt.yaml",
	}

	var tried []string
	for _, path := range candidates {
		if path == "" {
			continue
		}
		tried = append(tried, path)

		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var cfg Tesira2MQTTConfig
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			log.Fatalf("Failed to parse YAML in %s: %v", path, err)
		}

		validateConfig(&cfg, path)
		config = &cfg
		log.Printf("Loaded config from %s", path)
		return
	}

	log.Fatalf("No configuration file found. Tried: %v", tried)
}
