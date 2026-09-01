// Package tesira maintains a Tesira Text Protocol session over a transport,
// exposing synchronous commands and attribute subscriptions.
package tesira

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/alufers/tesira2mqtt/internal/transport"
	"github.com/alufers/tesira2mqtt/internal/ttp"
)

// welcomeText is the banner the TTP server prints once the session is usable.
const welcomeText = "Welcome to the Tesira Text Protocol Server"

// ErrNotConnected is returned when a command is issued with no live session.
var ErrNotConnected = errors.New("tesira: not connected")

// CommandError is a "-ERR" reply from the device. Callers inspect it on
// purpose - block discovery reads the block type and the valid index bounds out
// of deliberately invalid commands - so it is a typed error, not just text.
type CommandError struct {
	Command  string
	Message  string
	Response *ttp.Response
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("tesira: command %q failed: %s", e.Command, e.Message)
}

// AsCommandError reports whether err is a device "-ERR" reply, and returns it.
func AsCommandError(err error) (*CommandError, bool) {
	var ce *CommandError
	if errors.As(err, &ce) {
		return ce, true
	}
	return nil, false
}

// DeviceInfo holds the identity of the connected DSP.
type DeviceInfo struct {
	Hostname     string
	SerialNumber string
	Version      string
	Aliases      []string
}

// Options configures a Client.
type Options struct {
	Dialer transport.Dialer
	Log    *slog.Logger

	WelcomeTimeout      time.Duration
	CommandTimeout      time.Duration
	ReconnectInterval   time.Duration
	ResubscribeInterval time.Duration

	// OnReady runs after every successful (re)connection, once the session
	// baseline is applied and device info is known. It may issue commands. The
	// session is torn down and retried if it returns an error.
	OnReady func(ctx context.Context, c *Client) error

	// OnDisconnect runs after the session drops, before the reconnect delay.
	OnDisconnect func()
}

// Client owns one TTP session and reconnects it for the life of its context.
type Client struct {
	opts Options
	log  *slog.Logger

	// cmdMu serialises commands: the protocol has no request ids, so only one
	// command may be in flight at a time.
	cmdMu sync.Mutex

	connMu sync.Mutex
	conn   io.ReadWriteCloser

	respCh    chan *ttp.Response
	welcomeCh chan struct{}

	infoMu sync.RWMutex
	info   DeviceInfo

	subMu     sync.Mutex
	subs      map[string]*subscription // keyed by logical key
	subsByTok map[string]*subscription // keyed by publish token
	subSeq    int
}

// New builds a Client. Call Run to start it.
func New(opts Options) *Client {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.WelcomeTimeout <= 0 {
		opts.WelcomeTimeout = 10 * time.Second
	}
	if opts.CommandTimeout <= 0 {
		opts.CommandTimeout = 5 * time.Second
	}
	if opts.ReconnectInterval <= 0 {
		opts.ReconnectInterval = 5 * time.Second
	}
	if opts.ResubscribeInterval <= 0 {
		opts.ResubscribeInterval = 60 * time.Second
	}
	return &Client{
		opts:      opts,
		log:       opts.Log,
		respCh:    make(chan *ttp.Response, 8),
		subs:      map[string]*subscription{},
		subsByTok: map[string]*subscription{},
	}
}

// Info returns the identity of the connected device.
func (c *Client) Info() DeviceInfo {
	c.infoMu.RLock()
	defer c.infoMu.RUnlock()
	return c.info
}

// Run connects, serves the session, and reconnects until ctx is cancelled.
func (c *Client) Run(ctx context.Context) {
	for ctx.Err() == nil {
		start := time.Now()
		err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.log.Warn("tesira session ended", "endpoint", c.opts.Dialer.String(),
				"uptime", time.Since(start).Round(time.Second), "err", err)
		}
		if c.opts.OnDisconnect != nil {
			c.opts.OnDisconnect()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.opts.ReconnectInterval):
		}
	}
}

// session runs a single connection from dial to disconnect.
func (c *Client) session(ctx context.Context) error {
	dialCtx, cancelDial := context.WithTimeout(ctx, c.opts.WelcomeTimeout)
	conn, err := c.opts.Dialer.Dial(dialCtx)
	cancelDial()
	if err != nil {
		return err
	}
	defer conn.Close()

	c.setConn(conn)
	defer c.setConn(nil)

	// A fresh welcome channel per session: the banner from a previous
	// connection must never satisfy this one.
	c.welcomeCh = make(chan struct{})
	c.drainResponses()

	readErr := make(chan error, 1)
	go func() { readErr <- c.readLoop(conn) }()

	select {
	case <-c.welcomeCh:
	case err := <-readErr:
		return fmt.Errorf("connection lost before welcome banner: %w", err)
	case <-time.After(c.opts.WelcomeTimeout):
		return errors.New("timed out waiting for the TTP welcome banner")
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := c.applySessionBaseline(ctx); err != nil {
		return err
	}
	if err := c.loadDeviceInfo(ctx); err != nil {
		return err
	}

	info := c.Info()
	c.log.Info("tesira session established",
		"endpoint", c.opts.Dialer.String(), "hostname", info.Hostname,
		"serial", info.SerialNumber, "version", info.Version, "aliases", len(info.Aliases))

	sessionCtx, cancelSession := context.WithCancel(ctx)
	defer cancelSession()

	ready := make(chan error, 1)
	go func() {
		if c.opts.OnReady == nil {
			ready <- nil
			return
		}
		ready <- c.opts.OnReady(sessionCtx, c)
	}()

	select {
	case err := <-ready:
		if err != nil {
			return fmt.Errorf("session setup: %w", err)
		}
	case err := <-readErr:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}

	go c.resubscribeLoop(sessionCtx)

	select {
	case err := <-readErr:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// applySessionBaseline configures the session's response format. These settings
// live on the connection, so they are reapplied on every reconnect before any
// other command runs.
//
//	verbose true          - values come back labelled, as "value":X
//	detailedResponse true - every response carries "[ <command> ]", which is
//	                        what lets a response be tied to its request
func (c *Client) applySessionBaseline(ctx context.Context) error {
	for _, cmd := range []string{
		"SESSION set verbose true",
		"SESSION set detailedResponse true",
	} {
		if _, err := c.Command(ctx, cmd); err != nil {
			return fmt.Errorf("session baseline: %w", err)
		}
	}
	return nil
}

func (c *Client) loadDeviceInfo(ctx context.Context) error {
	info := DeviceInfo{}

	for _, q := range []struct {
		cmd  string
		dest *string
	}{
		{"DEVICE get hostname", &info.Hostname},
		{"DEVICE get serialNumber", &info.SerialNumber},
		{"DEVICE get version", &info.Version},
	} {
		res, err := c.Command(ctx, q.cmd)
		if err != nil {
			return err
		}
		s, err := res.Str()
		if err != nil {
			return fmt.Errorf("%s: %w", q.cmd, err)
		}
		*q.dest = s
	}

	res, err := c.Command(ctx, "SESSION get aliases")
	if err != nil {
		return err
	}
	if info.Aliases, err = res.Strings(); err != nil {
		return fmt.Errorf("SESSION get aliases: %w", err)
	}

	c.infoMu.Lock()
	c.info = info
	c.infoMu.Unlock()
	return nil
}

// Command sends one command and waits for its response. Commands are
// serialised; a "-ERR" reply is returned as a *CommandError.
//
// Never call this from a subscription callback: callbacks run on the reader
// goroutine, which must keep draining the connection for a response to arrive.
func (c *Client) Command(ctx context.Context, cmd string) (*ttp.Response, error) {
	c.cmdMu.Lock()
	defer c.cmdMu.Unlock()

	conn := c.getConn()
	if conn == nil {
		return nil, ErrNotConnected
	}

	// Discard anything left over from a command that timed out, so a late
	// response can never be mistaken for this one's.
	c.drainResponses()

	if _, err := io.WriteString(conn, cmd+"\n"); err != nil {
		return nil, fmt.Errorf("tesira: write %q: %w", cmd, err)
	}

	deadline := time.NewTimer(c.opts.CommandTimeout)
	defer deadline.Stop()

	for {
		select {
		case res := <-c.respCh:
			// detailedResponse makes the device echo the originating command,
			// so a response that belongs to someone else can be recognised and
			// dropped instead of being handed back as this command's answer.
			if res.Command != "" && !sameCommand(res.Command, cmd) {
				c.log.Debug("discarding unmatched tesira response",
					"sent", cmd, "echoed", res.Command, "raw", res.Raw)
				continue
			}
			c.log.Debug("tesira command", "cmd", cmd, "response", res.Raw)
			if res.Kind == ttp.KindError {
				return res, &CommandError{Command: cmd, Message: res.Error, Response: res}
			}
			return res, nil
		case <-deadline.C:
			return nil, fmt.Errorf("tesira: timeout waiting for response to %q", cmd)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Commandf is Command with printf-style formatting of the command line.
func (c *Client) Commandf(ctx context.Context, format string, args ...any) (*ttp.Response, error) {
	return c.Command(ctx, fmt.Sprintf(format, args...))
}

// sameCommand compares an echoed command with the one that was sent, ignoring
// differences in whitespace runs.
func sameCommand(echoed, sent string) bool {
	return strings.Join(strings.Fields(echoed), " ") == strings.Join(strings.Fields(sent), " ")
}

// readLoop consumes device output until the connection fails.
func (c *Client) readLoop(conn io.Reader) error {
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, welcomeText) {
			c.signalWelcome()
		}

		res, ok := ttp.ParseLine(line)
		if !ok {
			continue
		}

		if res.Kind == ttp.KindPublish {
			c.dispatchPublish(res)
			continue
		}

		select {
		case c.respCh <- res:
		default:
			c.log.Debug("tesira response queue full, dropping line", "raw", res.Raw)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return io.EOF
}

func (c *Client) signalWelcome() {
	defer func() {
		// Closing an already closed channel means a device that reprinted the
		// banner mid-session; that is harmless and must not kill the reader.
		_ = recover()
	}()
	select {
	case <-c.welcomeCh:
	default:
		close(c.welcomeCh)
	}
}

func (c *Client) drainResponses() {
	for {
		select {
		case <-c.respCh:
		default:
			return
		}
	}
}

func (c *Client) setConn(conn io.ReadWriteCloser) {
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
}

func (c *Client) getConn() io.ReadWriteCloser {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn
}
