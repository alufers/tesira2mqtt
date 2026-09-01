package tesira_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alufers/tesira2mqtt/internal/fakedsp"
	"github.com/alufers/tesira2mqtt/internal/tesira"
	"github.com/alufers/tesira2mqtt/internal/transport"
	"github.com/alufers/tesira2mqtt/internal/ttp"
)

// newTestClient wires a Client to a fake DSP over the telnet transport, which
// also exercises the IAC negotiation filter.
func newTestClient(t *testing.T, dsp *fakedsp.Server, onReady func(context.Context, *tesira.Client) error) *tesira.Client {
	t.Helper()
	host, port := dsp.Addr()
	return tesira.New(tesira.Options{
		Dialer: transport.NewTelnetDialer(transport.TelnetConfig{
			Host: host, Port: port, Timeout: 2 * time.Second,
		}),
		Log:                 testLogger(t),
		WelcomeTimeout:      2 * time.Second,
		CommandTimeout:      2 * time.Second,
		ReconnectInterval:   50 * time.Millisecond,
		ResubscribeInterval: time.Hour,
		OnReady:             onReady,
	})
}

func newTestDSP(t *testing.T) *fakedsp.Server {
	t.Helper()
	dsp, err := fakedsp.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dsp.Close)

	dsp.SetValue("DEVICE get hostname", `"fake-tesira"`)
	dsp.SetValue("DEVICE get serialNumber", `"00000001"`)
	dsp.SetValue("DEVICE get version", `"5.7.0.12"`)
	dsp.SetList("SESSION get aliases", `["DEVICE" "level_one" "RoomCombiner1" "Mystery1"]`)

	dsp.SetError(`"level_one" get BLOCKTYPE`, `'BLOCKTYPE' is not supported by LevelControlInterface::Attributes`)
	dsp.SetError(`"RoomCombiner1" get BLOCKTYPE`, `'BLOCKTYPE' is not supported by RoomCombinerInterface::Attributes`)
	dsp.SetError(`"Mystery1" get BLOCKTYPE`, `'BLOCKTYPE' is not supported by SomethingElseInterface::Attributes`)
	return dsp
}

func TestClientConnectsAndReadsDeviceInfo(t *testing.T) {
	dsp := newTestDSP(t)

	ready := make(chan struct{}, 4)
	c := newTestClient(t, dsp, func(context.Context, *tesira.Client) error {
		ready <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	waitFor(t, ready)

	info := c.Info()
	if info.Hostname != "fake-tesira" {
		t.Errorf("hostname = %q, want fake-tesira", info.Hostname)
	}
	if info.SerialNumber != "00000001" {
		t.Errorf("serial = %q", info.SerialNumber)
	}
	if info.Version != "5.7.0.12" {
		t.Errorf("version = %q", info.Version)
	}
	if len(info.Aliases) != 4 {
		t.Errorf("aliases = %v", info.Aliases)
	}
}

// The session baseline must be reapplied on every connection, because those
// settings live on the connection and vanish with it.
func TestSessionBaselineIsAppliedOnEveryConnection(t *testing.T) {
	dsp := newTestDSP(t)

	ready := make(chan struct{}, 4)
	c := newTestClient(t, dsp, func(context.Context, *tesira.Client) error {
		ready <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	waitFor(t, ready)
	dsp.DropConnections()
	waitFor(t, ready)

	if got := dsp.Sessions(); got < 2 {
		t.Fatalf("sessions = %d, want at least 2", got)
	}

	var verbose, detailed int
	for _, cmd := range dsp.Commands() {
		switch cmd {
		case "SESSION set verbose true":
			verbose++
		case "SESSION set detailedResponse true":
			detailed++
		}
	}
	if verbose < 2 || detailed < 2 {
		t.Errorf("baseline applied %d/%d times across %d sessions, want once per session",
			verbose, detailed, dsp.Sessions())
	}

	// The baseline must come before anything else on the connection.
	cmds := dsp.Commands()
	if cmds[0] != "SESSION set verbose true" || cmds[1] != "SESSION set detailedResponse true" {
		t.Errorf("session opened with %q, %q; want the baseline first", cmds[0], cmds[1])
	}
}

func TestDiscoverBlocks(t *testing.T) {
	dsp := newTestDSP(t)

	type result struct {
		blocks []tesira.BlockInfo
		err    error
	}
	results := make(chan result, 2)
	c := newTestClient(t, dsp, func(ctx context.Context, c *tesira.Client) error {
		b, err := c.DiscoverBlocks(ctx)
		results <- result{b, err}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	var got result
	select {
	case got = <-results:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for discovery")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}

	want := map[string]string{
		"level_one":     "LevelControl",
		"RoomCombiner1": "RoomCombiner",
		"Mystery1":      "SomethingElse",
	}
	if len(got.blocks) != len(want) {
		t.Fatalf("discovered %d blocks, want %d: %+v", len(got.blocks), len(want), got.blocks)
	}
	for _, b := range got.blocks {
		if want[b.ID] != b.Type {
			t.Errorf("block %s type = %q, want %q", b.ID, b.Type, want[b.ID])
		}
	}
	// "DEVICE" is the device handle, not a DSP block.
	if dsp.Received(`"DEVICE" get BLOCKTYPE`) {
		t.Error("DEVICE should not be probed for a block type")
	}
}

func TestIndexRangeReadsBoundsFromError(t *testing.T) {
	dsp := newTestDSP(t)
	dsp.SetError(`"RoomCombiner1" get wallState 0`,
		`INVALID_PARAMETER Index out of range:wallId min:1 max:8 received:0`)

	got := make(chan ttp.IndexRange, 1)
	c := newTestClient(t, dsp, func(ctx context.Context, c *tesira.Client) error {
		r, err := c.IndexRange(ctx, "RoomCombiner1", "wallState")
		if err != nil {
			return err
		}
		got <- r
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	select {
	case r := <-got:
		if r.Min != 1 || r.Max != 8 {
			t.Errorf("range = %+v, want 1..8", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestSubscriptionsRouteByToken(t *testing.T) {
	dsp := newTestDSP(t)
	dsp.SetSubscriptionValue("levels", "ALL", `[-30.5 -30.5]`)
	dsp.SetSubscriptionValue("wallState", "1", `true`)

	var mu sync.Mutex
	levels := []float64(nil)
	walls := []bool(nil)
	updated := make(chan struct{}, 16)

	one := 1
	c := newTestClient(t, dsp, func(ctx context.Context, c *tesira.Client) error {
		if err := c.Subscribe(ctx, "level_one", "levels", nil, func(res *ttp.Response) {
			v, err := res.Floats()
			if err != nil {
				t.Errorf("levels payload: %v", err)
				return
			}
			mu.Lock()
			levels = v
			mu.Unlock()
			updated <- struct{}{}
		}); err != nil {
			return err
		}
		return c.Subscribe(ctx, "RoomCombiner1", "wallState", &one, func(res *ttp.Response) {
			v, err := res.Bool()
			if err != nil {
				t.Errorf("wallState payload: %v", err)
				return
			}
			mu.Lock()
			walls = []bool{v}
			mu.Unlock()
			updated <- struct{}{}
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	waitFor(t, updated)
	waitFor(t, updated)

	mu.Lock()
	if len(levels) != 2 || levels[0] != -30.5 {
		t.Errorf("levels = %v, want [-30.5 -30.5]", levels)
	}
	if len(walls) != 1 || !walls[0] {
		t.Errorf("walls = %v, want [true]", walls)
	}
	mu.Unlock()

	// A change pushed by the device reaches the right handler.
	dsp.PublishUpdate("levels", "ALL", `[-12 -12]`)
	waitFor(t, updated)

	mu.Lock()
	defer mu.Unlock()
	if len(levels) != 2 || levels[0] != -12 {
		t.Errorf("levels after update = %v, want [-12 -12]", levels)
	}
}

// A live subscription is declined with ALREADY_SUBSCRIBED, which means healthy,
// not broken.
func TestResubscribeToleratesAlreadySubscribed(t *testing.T) {
	dsp := newTestDSP(t)
	dsp.SetSubscriptionValue("levels", "ALL", `[-30.5]`)

	errs := make(chan error, 2)
	c := newTestClient(t, dsp, func(ctx context.Context, c *tesira.Client) error {
		if err := c.Subscribe(ctx, "level_one", "levels", nil, func(*ttp.Response) {}); err != nil {
			return err
		}
		// Same subscription again, then a full refresh pass.
		if err := c.Subscribe(ctx, "level_one", "levels", nil, func(*ttp.Response) {}); err != nil {
			errs <- err
			return nil
		}
		errs <- c.Resubscribe(ctx)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("resubscribe reported an error for a healthy subscription: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestCommandErrorIsTyped(t *testing.T) {
	dsp := newTestDSP(t)

	errs := make(chan error, 1)
	c := newTestClient(t, dsp, func(ctx context.Context, c *tesira.Client) error {
		_, err := c.Get(ctx, "level_one", "nonsense")
		errs <- err
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	select {
	case err := <-errs:
		cerr, ok := tesira.AsCommandError(err)
		if !ok {
			t.Fatalf("error %v is not a *CommandError", err)
		}
		if cerr.Message == "" {
			t.Error("command error carries no message")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestReconnectsAfterDrop(t *testing.T) {
	dsp := newTestDSP(t)

	ready := make(chan struct{}, 8)
	c := newTestClient(t, dsp, func(context.Context, *tesira.Client) error {
		ready <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	waitFor(t, ready)
	dsp.DropConnections()
	waitFor(t, ready)
	dsp.DropConnections()
	waitFor(t, ready)

	if got := dsp.Sessions(); got < 3 {
		t.Errorf("sessions = %d, want at least 3", got)
	}
}

func waitFor(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the client")
	}
}
