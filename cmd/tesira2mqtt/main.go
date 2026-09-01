// Command tesira2mqtt bridges Biamp Tesira DSPs to MQTT over the Tesira Text
// Protocol.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/alufers/tesira2mqtt/internal/blocks"
	"github.com/alufers/tesira2mqtt/internal/bridge"
	"github.com/alufers/tesira2mqtt/internal/config"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	configPath := flag.String("config", "/config.yaml", "path to the YAML configuration file")
	logLevel := flag.String("log-level", "", "override log_level from the config file")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tesira2mqtt:", err)
		os.Exit(1)
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}

	log := newLogger(cfg.LogLevel)
	log.Info("starting tesira2mqtt", "version", version,
		"systems", len(cfg.Systems), "block_types", strings.Join(blocks.SupportedTypes(), ","))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	bridges := make([]*bridge.Bridge, 0, len(cfg.Systems))
	var wg sync.WaitGroup
	for _, sys := range cfg.Systems {
		b := bridge.New(sys, cfg.MQTT, log)
		bridges = append(bridges, b)
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Run(ctx)
		}()
	}

	<-ctx.Done()
	log.Info("shutting down")
	wg.Wait()
	for _, b := range bridges {
		b.Shutdown()
	}
	log.Info("stopped")
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
