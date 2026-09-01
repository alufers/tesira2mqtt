// Package mqttx wraps the Paho MQTT client with the conveniences this bridge
// needs: topic scoping, retained-publish deduplication, and re-subscription
// after a broker reconnect.
package mqttx

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Options configures a Client.
type Options struct {
	Broker   string
	ClientID string
	Username string
	Password string

	QoS    byte
	Retain bool

	KeepAlive      time.Duration
	ConnectTimeout time.Duration
	InsecureTLS    bool

	// WillTopic and WillPayload configure the broker's last will, which marks
	// this bridge offline if it dies without saying goodbye.
	WillTopic   string
	WillPayload string

	Log *slog.Logger

	// OnConnect runs after every successful connection to the broker, once
	// subscriptions have been restored. Use it to republish state.
	OnConnect func()
}

// Client is a connected MQTT session.
type Client struct {
	opts Options
	log  *slog.Logger
	mc   mqtt.Client

	mu     sync.Mutex
	last   map[string]string       // topic -> last published payload
	subs   map[string]func(string) // topic -> handler
	closed bool
}

// New builds a Client. Call Connect to start it.
func New(opts Options) *Client {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.KeepAlive <= 0 {
		opts.KeepAlive = 30 * time.Second
	}
	if opts.ConnectTimeout <= 0 {
		opts.ConnectTimeout = 10 * time.Second
	}

	c := &Client{
		opts: opts,
		log:  opts.Log,
		last: map[string]string{},
		subs: map[string]func(string){},
	}

	mo := mqtt.NewClientOptions().
		AddBroker(opts.Broker).
		SetClientID(opts.ClientID).
		SetUsername(opts.Username).
		SetPassword(opts.Password).
		SetKeepAlive(opts.KeepAlive).
		SetConnectTimeout(opts.ConnectTimeout).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(30 * time.Second).
		SetCleanSession(true).
		SetOrderMatters(false)

	if opts.InsecureTLS {
		mo.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	if opts.WillTopic != "" {
		mo.SetWill(opts.WillTopic, opts.WillPayload, opts.QoS, true)
	}

	mo.SetOnConnectHandler(func(mqtt.Client) {
		c.log.Info("mqtt connected", "broker", opts.Broker, "client_id", opts.ClientID)
		c.onConnect()
	})
	mo.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		c.log.Warn("mqtt connection lost", "broker", opts.Broker, "err", err)
	})

	c.mc = mqtt.NewClient(mo)
	return c
}

// Connect establishes the broker session.
func (c *Client) Connect(ctx context.Context) error {
	tok := c.mc.Connect()
	if !waitToken(ctx, tok, c.opts.ConnectTimeout) {
		return fmt.Errorf("mqtt: connect to %s timed out", c.opts.Broker)
	}
	if err := tok.Error(); err != nil {
		return fmt.Errorf("mqtt: connect to %s: %w", c.opts.Broker, err)
	}
	return nil
}

// onConnect restores subscriptions and clears the dedup cache, so the caller's
// republish after a reconnect is unconditional.
func (c *Client) onConnect() {
	c.mu.Lock()
	c.last = map[string]string{}
	subs := make(map[string]func(string), len(c.subs))
	for topic, handler := range c.subs {
		subs[topic] = handler
	}
	c.mu.Unlock()

	for topic, handler := range subs {
		c.subscribe(topic, handler)
	}
	if c.opts.OnConnect != nil {
		go c.opts.OnConnect()
	}
}

// Publish sends a retained state update, skipping topics whose value has not
// changed since the last publish.
func (c *Client) Publish(topic, payload string) {
	c.mu.Lock()
	if prev, ok := c.last[topic]; ok && prev == payload {
		c.mu.Unlock()
		return
	}
	c.last[topic] = payload
	c.mu.Unlock()

	c.PublishRaw(topic, payload, c.opts.Retain)
}

// PublishRaw sends a payload unconditionally.
func (c *Client) PublishRaw(topic, payload string, retain bool) {
	if !c.mc.IsConnected() {
		c.log.Debug("mqtt not connected, dropping publish", "topic", topic)
		return
	}
	c.log.Debug("mqtt publish", "topic", topic, "payload", payload)
	c.mc.Publish(topic, c.opts.QoS, retain, payload)
}

// PublishSync sends a payload and waits for it to leave the client, which
// matters for the final "offline" message during shutdown.
func (c *Client) PublishSync(topic, payload string, retain bool, timeout time.Duration) {
	if !c.mc.IsConnected() {
		return
	}
	tok := c.mc.Publish(topic, c.opts.QoS, retain, payload)
	tok.WaitTimeout(timeout)
}

// Subscribe registers a handler for a topic and subscribes to it. The handler
// is re-registered automatically after a broker reconnect.
func (c *Client) Subscribe(topic string, handler func(payload string)) {
	c.mu.Lock()
	c.subs[topic] = handler
	c.mu.Unlock()
	c.subscribe(topic, handler)
}

func (c *Client) subscribe(topic string, handler func(payload string)) {
	if !c.mc.IsConnected() {
		return
	}
	tok := c.mc.Subscribe(topic, c.opts.QoS, func(_ mqtt.Client, m mqtt.Message) {
		payload := string(m.Payload())
		c.log.Debug("mqtt command", "topic", m.Topic(), "payload", payload)
		handler(payload)
	})
	go func() {
		if !tok.WaitTimeout(c.opts.ConnectTimeout) {
			c.log.Warn("mqtt subscribe timed out", "topic", topic)
			return
		}
		if err := tok.Error(); err != nil {
			c.log.Warn("mqtt subscribe failed", "topic", topic, "err", err)
		}
	}()
}

// ForgetPublished clears the dedup cache so the next publish always goes out.
func (c *Client) ForgetPublished() {
	c.mu.Lock()
	c.last = map[string]string{}
	c.mu.Unlock()
}

// Close disconnects from the broker.
func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()
	c.mc.Disconnect(250)
}

func waitToken(ctx context.Context, tok mqtt.Token, timeout time.Duration) bool {
	select {
	case <-tok.Done():
		return true
	case <-ctx.Done():
		return false
	case <-time.After(timeout):
		return false
	}
}

// Scope binds a topic prefix to a client, so blocks can publish relative paths.
type Scope struct {
	c      *Client
	prefix string
}

// Scope returns a publisher rooted at prefix.
func (c *Client) Scope(prefix string) *Scope {
	return &Scope{c: c, prefix: strings.TrimSuffix(prefix, "/")}
}

// Child returns a scope nested below this one.
func (s *Scope) Child(rel string) *Scope {
	return &Scope{c: s.c, prefix: s.Topic(rel)}
}

// Topic renders a relative path as an absolute topic.
func (s *Scope) Topic(rel string) string {
	rel = strings.Trim(rel, "/")
	if rel == "" {
		return s.prefix
	}
	if s.prefix == "" {
		return rel
	}
	return s.prefix + "/" + rel
}

// Publish sends a retained, deduplicated state update.
func (s *Scope) Publish(rel, payload string) { s.c.Publish(s.Topic(rel), payload) }

// Subscribe registers a handler for a relative topic.
func (s *Scope) Subscribe(rel string, handler func(payload string)) {
	s.c.Subscribe(s.Topic(rel), handler)
}
