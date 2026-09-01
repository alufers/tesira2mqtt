// Package bridge joins one Tesira system to MQTT.
package bridge

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/alufers/tesira2mqtt/internal/blocks"
	"github.com/alufers/tesira2mqtt/internal/config"
	"github.com/alufers/tesira2mqtt/internal/mqttx"
	"github.com/alufers/tesira2mqtt/internal/tesira"
	"github.com/alufers/tesira2mqtt/internal/transport"
)

// Status payloads published on <prefix>/<system>/status.
const (
	statusOnline  = "online"
	statusOffline = "offline"
)

// Bridge mirrors one Tesira system onto MQTT.
type Bridge struct {
	sys config.System
	mq  config.MQTT
	log *slog.Logger

	client *tesira.Client

	mu         sync.Mutex
	name       string
	mqtt       *mqttx.Client
	scope      *mqttx.Scope
	blocks     map[string]blocks.Block
	discovered bool

	ctx context.Context
}

// New builds a Bridge for one configured system.
func New(sys config.System, mq config.MQTT, log *slog.Logger) *Bridge {
	b := &Bridge{
		sys:    sys,
		mq:     mq,
		log:    log.With("system", systemLogName(sys)),
		blocks: map[string]blocks.Block{},
	}

	b.client = tesira.New(tesira.Options{
		Dialer:              b.dialer(),
		Log:                 b.log,
		WelcomeTimeout:      sys.ConnectTimeout,
		CommandTimeout:      sys.CommandTimeout,
		ReconnectInterval:   sys.ReconnectInterval,
		ResubscribeInterval: sys.ResubscribeInterval,
		OnReady:             b.onReady,
		OnDisconnect:        b.onDisconnect,
	})
	return b
}

func (b *Bridge) dialer() transport.Dialer {
	if b.sys.Transport == config.TransportTelnet {
		return transport.NewTelnetDialer(transport.TelnetConfig{
			Host:    b.sys.Host,
			Port:    b.sys.Port,
			Timeout: b.sys.ConnectTimeout,
		})
	}
	return transport.NewSSHDialer(transport.SSHConfig{
		Host:           b.sys.Host,
		Port:           b.sys.Port,
		Username:       b.sys.Username,
		Password:       b.sys.Password,
		VerifyHostKey:  b.sys.HostKeyVerification,
		KnownHostsFile: b.sys.KnownHostsFile,
		Timeout:        b.sys.ConnectTimeout,
	})
}

// Run bridges the system until ctx is cancelled.
func (b *Bridge) Run(ctx context.Context) {
	b.mu.Lock()
	b.ctx = ctx
	b.mu.Unlock()

	b.client.Run(ctx)
}

// onReady runs after every successful Tesira connection.
func (b *Bridge) onReady(ctx context.Context, c *tesira.Client) error {
	if err := b.ensureMQTT(ctx); err != nil {
		return err
	}

	if err := b.ensureBlocks(ctx, c); err != nil {
		return err
	}
	if err := b.syncBlocks(ctx); err != nil {
		return err
	}

	b.scopeRef().Publish("status", statusOnline)
	return nil
}

func (b *Bridge) onDisconnect() {
	if scope := b.scopeRef(); scope != nil {
		scope.Publish("status", statusOffline)
	}
}

// ensureMQTT connects to the broker on the first Tesira connection. It is
// deferred until then because the system's topic segment defaults to the
// device's own hostname, which is only known once the DSP has answered - and
// the last-will topic must be fixed before the broker session opens.
func (b *Bridge) ensureMQTT(ctx context.Context) error {
	b.mu.Lock()
	if b.mqtt != nil {
		b.mu.Unlock()
		return nil
	}

	name := b.sys.Name
	if name == "" {
		name = b.client.Info().Hostname
	}
	if name == "" {
		name = b.sys.Host
	}
	name = TopicSegment(name)
	b.name = name
	b.log = b.log.With("name", name)

	systemPrefix := b.mq.Prefix + "/" + name
	b.mu.Unlock()

	client := mqttx.New(mqttx.Options{
		Broker:         b.mq.Broker,
		ClientID:       fmt.Sprintf("%s-%s", b.mq.ClientID, name),
		Username:       b.mq.Username,
		Password:       b.mq.Password,
		QoS:            b.mq.QoS,
		Retain:         b.mq.Retain,
		KeepAlive:      b.mq.KeepAlive,
		ConnectTimeout: b.mq.ConnectTimeout,
		InsecureTLS:    b.mq.InsecureTLS,
		WillTopic:      systemPrefix + "/status",
		WillPayload:    statusOffline,
		Log:            b.log,
		OnConnect:      b.onMQTTConnect,
	})

	if err := client.Connect(ctx); err != nil {
		return err
	}

	b.mu.Lock()
	b.mqtt = client
	b.scope = client.Scope(systemPrefix)
	b.mu.Unlock()

	b.log.Info("bridging system", "topic_prefix", systemPrefix)
	return nil
}

// onMQTTConnect republishes everything after a broker reconnect, since a fresh
// session means the broker may have lost our retained state and our
// subscriptions have just been restored.
func (b *Bridge) onMQTTConnect() {
	b.mu.Lock()
	ctx, ready := b.ctx, b.discovered
	b.mu.Unlock()

	if ctx == nil || !ready {
		return // first connect: onReady publishes everything anyway
	}
	if err := b.syncBlocks(ctx); err != nil {
		b.log.Warn("republish after mqtt reconnect failed", "err", err)
		return
	}
	b.scopeRef().Publish("status", statusOnline)
}

// ensureBlocks discovers the DSP's blocks once, on the first connection.
func (b *Bridge) ensureBlocks(ctx context.Context, c *tesira.Client) error {
	b.mu.Lock()
	done := b.discovered
	b.mu.Unlock()
	if done {
		return nil
	}

	found, err := c.DiscoverBlocks(ctx)
	if err != nil {
		return fmt.Errorf("discover blocks: %w", err)
	}

	built := map[string]blocks.Block{}
	skipped := 0
	for _, info := range found {
		if !b.wantBlock(info) {
			skipped++
			continue
		}

		scope := b.scopeRef().Child(TopicSegment(info.ID))
		block, ok := blocks.New(info.Type, blocks.Deps{
			Log:    b.log,
			Client: c,
			ID:     info.ID,
			Type:   info.Type,
			Pub:    scope,
		})
		if !ok {
			b.log.Debug("unsupported block type", "block", info.ID, "type", info.Type)
			skipped++
			continue
		}

		if err := block.Discover(ctx); err != nil {
			// One malformed block must not take the whole system down.
			b.log.Warn("block discovery failed", "block", info.ID, "type", info.Type, "err", err)
			continue
		}

		scope.Publish("type", info.Type)
		built[info.ID] = block
	}

	b.mu.Lock()
	b.blocks = built
	b.discovered = true
	b.mu.Unlock()

	b.log.Info("blocks ready", "bridged", len(built), "skipped", skipped,
		"discovered", len(found), "supported_types", strings.Join(blocks.SupportedTypes(), ","))
	return nil
}

// wantBlock applies the configured allow and deny lists.
func (b *Bridge) wantBlock(info tesira.BlockInfo) bool {
	if slices.Contains(b.sys.SkipBlocks, info.ID) {
		return false
	}
	if len(b.sys.BlockTypes) > 0 && !slices.Contains(b.sys.BlockTypes, info.Type) {
		return false
	}
	return true
}

// syncBlocks re-establishes subscriptions and republishes state.
func (b *Bridge) syncBlocks(ctx context.Context) error {
	b.mu.Lock()
	list := make([]struct {
		id string
		bl blocks.Block
	}, 0, len(b.blocks))
	for id, bl := range b.blocks {
		list = append(list, struct {
			id string
			bl blocks.Block
		}{id, bl})
	}
	b.mu.Unlock()

	for _, item := range list {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := item.bl.Sync(ctx); err != nil {
			return fmt.Errorf("sync block %s: %w", item.id, err)
		}
	}
	return nil
}

// Shutdown marks the system offline and closes the broker session.
func (b *Bridge) Shutdown() {
	b.mu.Lock()
	client, scope := b.mqtt, b.scope
	b.mu.Unlock()

	if client == nil {
		return
	}
	if scope != nil {
		client.PublishSync(scope.Topic("status"), statusOffline, true, 2*time.Second)
	}
	client.Close()
}

func (b *Bridge) scopeRef() *mqttx.Scope {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.scope
}

// TopicSegment makes a device or block name safe to use as one MQTT topic
// level. Tesira forbids "/" in instance tags but allows the MQTT wildcards.
func TopicSegment(s string) string {
	replacer := strings.NewReplacer("/", "_", "+", "_", "#", "_")
	out := strings.TrimSpace(replacer.Replace(s))
	if out == "" {
		return "unnamed"
	}
	return out
}

func systemLogName(sys config.System) string {
	if sys.Name != "" {
		return sys.Name
	}
	return sys.Host
}
