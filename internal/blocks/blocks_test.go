package blocks_test

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alufers/tesira2mqtt/internal/blocks"
	"github.com/alufers/tesira2mqtt/internal/fakedsp"
	"github.com/alufers/tesira2mqtt/internal/tesira"
	"github.com/alufers/tesira2mqtt/internal/transport"
)

// recPub records what a block publishes and lets tests fire /set commands.
type recPub struct {
	mu      sync.Mutex
	values  map[string]string
	handler map[string]func(string)
	updated chan struct{}
}

func newRecPub() *recPub {
	return &recPub{
		values:  map[string]string{},
		handler: map[string]func(string){},
		updated: make(chan struct{}, 256),
	}
}

func (p *recPub) Publish(rel, payload string) {
	p.mu.Lock()
	p.values[rel] = payload
	p.mu.Unlock()
	select {
	case p.updated <- struct{}{}:
	default:
	}
}

func (p *recPub) Subscribe(rel string, h func(string)) {
	p.mu.Lock()
	p.handler[rel] = h
	p.mu.Unlock()
}

func (p *recPub) get(rel string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v, ok := p.values[rel]
	return v, ok
}

func (p *recPub) send(t *testing.T, rel, payload string) {
	t.Helper()
	p.mu.Lock()
	h := p.handler[rel]
	p.mu.Unlock()
	if h == nil {
		t.Fatalf("no /set handler registered for %q", rel)
	}
	h(payload)
}

func (p *recPub) hasHandler(rel string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.handler[rel] != nil
}

// waitForValue polls until a topic holds want, so tests do not race the
// device's asynchronous updates.
func (p *recPub) waitForValue(t *testing.T, rel, want string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if got, ok := p.get(rel); ok && got == want {
			return
		}
		select {
		case <-p.updated:
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			got, _ := p.get(rel)
			t.Fatalf("%s = %q, want %q", rel, got, want)
		}
	}
}

// runBlock connects a client to the fake DSP and drives one block through
// Discover and Sync.
func runBlock(t *testing.T, dsp *fakedsp.Server, blockType, blockID string) (*recPub, *fakedsp.Server) {
	t.Helper()
	pub := newRecPub()

	host, port := dsp.Addr()
	done := make(chan error, 1)
	c := tesira.New(tesira.Options{
		Dialer: transport.NewTelnetDialer(transport.TelnetConfig{
			Host: host, Port: port, Timeout: 2 * time.Second,
		}),
		Log:                 slog.New(slog.NewTextHandler(discard{}, nil)),
		WelcomeTimeout:      2 * time.Second,
		CommandTimeout:      2 * time.Second,
		ReconnectInterval:   time.Hour,
		ResubscribeInterval: time.Hour,
		OnReady: func(ctx context.Context, c *tesira.Client) error {
			block, ok := blocks.New(blockType, blocks.Deps{
				Log:    slog.New(slog.NewTextHandler(discard{}, nil)),
				Client: c,
				ID:     blockID,
				Type:   blockType,
				Pub:    pub,
			})
			if !ok {
				done <- errUnsupported(blockType)
				return nil
			}
			if err := block.Discover(ctx); err != nil {
				done <- err
				return nil
			}
			done <- block.Sync(ctx)
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go c.Run(ctx)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("block setup: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out setting up block")
	}
	return pub, dsp
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

type errUnsupported string

func (e errUnsupported) Error() string { return "unsupported block type " + string(e) }

func newDSP(t *testing.T) *fakedsp.Server {
	t.Helper()
	dsp, err := fakedsp.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dsp.Close)
	dsp.SetValue("DEVICE get hostname", `"fake"`)
	dsp.SetValue("DEVICE get serialNumber", `"1"`)
	dsp.SetValue("DEVICE get version", `"5.7"`)
	dsp.SetList("SESSION get aliases", `["DEVICE"]`)
	return dsp
}

// levelDSP scripts a Level Control block with the given channel count.
func levelDSP(t *testing.T, id string, channels int, ganged bool, min, max string) *fakedsp.Server {
	t.Helper()
	dsp := newDSP(t)
	q := `"` + id + `"`
	dsp.SetValue(q+" get numChannels", itoa(channels))
	if ganged {
		dsp.SetValue(q+" get ganged", "true")
	} else {
		dsp.SetValue(q+" get ganged", "false")
	}
	for i := 1; i <= channels; i++ {
		dsp.SetValue(q+" get label "+itoa(i), `"Chan `+itoa(i)+`"`)
		dsp.SetValue(q+" get minLevel "+itoa(i), min)
		dsp.SetValue(q+" get maxLevel "+itoa(i), max)
	}
	return dsp
}

func itoa(i int) string { return strconv.Itoa(i) }

// A ganged block is one knob for the whole block, so its controls live at the
// block root and a write there drives every channel.
func TestLevelControlGangedPublishesAtRoot(t *testing.T) {
	dsp := levelDSP(t, "level_anc", 2, true, "-92.0", "-40.0")
	dsp.SetSubscriptionValue("levels", "ALL", `[-66.0 -66.0]`)
	dsp.SetSubscriptionValue("mutes", "ALL", `[false false]`)
	dsp.SetOK(`"level_anc" set level 1 -40`)
	dsp.SetOK(`"level_anc" set level 2 -40`)

	pub, _ := runBlock(t, dsp, "LevelControl", "level_anc")

	pub.waitForValue(t, "level_dB", "-66")
	pub.waitForValue(t, "mute", "false")
	// -66 sits halfway between -92 and -40.
	pub.waitForValue(t, "level_percent", "50")
	pub.waitForValue(t, "min_level_dB", "-92")
	pub.waitForValue(t, "max_level_dB", "-40")
	pub.waitForValue(t, "ganged", "true")
	pub.waitForValue(t, "channels/count", "2")

	if _, ok := pub.get("channels/1/level_dB"); ok {
		t.Error("a ganged block should not publish per-channel topics")
	}

	// A root write fans out to every channel.
	pub.send(t, "level_dB/set", "-40")
	waitForCommand(t, dsp, `"level_anc" set level 1 -40`)
	waitForCommand(t, dsp, `"level_anc" set level 2 -40`)
}

// A block that is not ganged exposes its channels individually.
func TestLevelControlUngangedPublishesPerChannel(t *testing.T) {
	dsp := levelDSP(t, "level_multi", 2, false, "-100.0", "0.0")
	dsp.SetSubscriptionValue("levels", "ALL", `[-50.0 -25.0]`)
	dsp.SetSubscriptionValue("mutes", "ALL", `[false true]`)
	dsp.SetOK(`"level_multi" set mute 2 false`)

	pub, _ := runBlock(t, dsp, "LevelControl", "level_multi")

	pub.waitForValue(t, "channels/1/level_dB", "-50")
	pub.waitForValue(t, "channels/2/level_dB", "-25")
	pub.waitForValue(t, "channels/1/level_percent", "50")
	pub.waitForValue(t, "channels/2/level_percent", "75")
	pub.waitForValue(t, "channels/1/mute", "false")
	pub.waitForValue(t, "channels/2/mute", "true")
	pub.waitForValue(t, "channels/2/label", "Chan 2")

	if _, ok := pub.get("level_dB"); ok {
		t.Error("an unganged multi-channel block should not publish root level topics")
	}
	if !pub.hasHandler("channels/2/mute/set") {
		t.Error("per-channel mute command topic is missing")
	}

	// A per-channel write targets only that channel.
	pub.send(t, "channels/2/mute/set", "off")
	waitForCommand(t, dsp, `"level_multi" set mute 2 false`)
	if dsp.Received(`"level_multi" set mute 1 false`) {
		t.Error("a per-channel write must not touch other channels")
	}
}

// A single-channel block is trivially one knob, so it uses the root topics too.
func TestLevelControlSingleChannelUsesRoot(t *testing.T) {
	dsp := levelDSP(t, "level_one", 1, false, "-60.0", "0.0")
	dsp.SetSubscriptionValue("levels", "ALL", `[-30.0]`)
	dsp.SetSubscriptionValue("mutes", "ALL", `[false]`)

	pub, _ := runBlock(t, dsp, "LevelControl", "level_one")

	pub.waitForValue(t, "level_dB", "-30")
	pub.waitForValue(t, "level_percent", "50")
	if _, ok := pub.get("channels/1/level_dB"); ok {
		t.Error("a single-channel block should not publish per-channel topics")
	}
}

func TestLevelControlPercentSetConvertsToDecibels(t *testing.T) {
	dsp := levelDSP(t, "level_one", 1, false, "-100.0", "0.0")
	dsp.SetSubscriptionValue("levels", "ALL", `[-50.0]`)
	dsp.SetSubscriptionValue("mutes", "ALL", `[false]`)
	dsp.SetOK(`"level_one" set level 1 -25`)

	pub, _ := runBlock(t, dsp, "LevelControl", "level_one")
	pub.waitForValue(t, "level_dB", "-50")

	pub.send(t, "level_percent/set", "75")
	waitForCommand(t, dsp, `"level_one" set level 1 -25`)
}

// Out-of-range writes are clamped to the block's own limits rather than being
// sent on for the DSP to reject.
func TestLevelControlClampsToBlockLimits(t *testing.T) {
	dsp := levelDSP(t, "level_one", 1, false, "-60.0", "-10.0")
	dsp.SetSubscriptionValue("levels", "ALL", `[-30.0]`)
	dsp.SetSubscriptionValue("mutes", "ALL", `[false]`)
	dsp.SetOK(`"level_one" set level 1 -10`)

	pub, _ := runBlock(t, dsp, "LevelControl", "level_one")
	pub.waitForValue(t, "level_dB", "-30")

	pub.send(t, "level_dB/set", "12")
	waitForCommand(t, dsp, `"level_one" set level 1 -10`)
}

// A device-side change reaches MQTT through the subscription, with no polling.
func TestLevelControlTracksDeviceUpdates(t *testing.T) {
	dsp := levelDSP(t, "level_one", 1, false, "-100.0", "0.0")
	dsp.SetSubscriptionValue("levels", "ALL", `[-50.0]`)
	dsp.SetSubscriptionValue("mutes", "ALL", `[false]`)

	pub, _ := runBlock(t, dsp, "LevelControl", "level_one")
	pub.waitForValue(t, "level_dB", "-50")

	dsp.PublishUpdate("levels", "ALL", `[-10.0]`)
	pub.waitForValue(t, "level_dB", "-10")
	pub.waitForValue(t, "level_percent", "90")

	dsp.PublishUpdate("mutes", "ALL", `[true]`)
	pub.waitForValue(t, "mute", "true")
}

func roomCombinerDSP(t *testing.T, id string, walls, rooms int, names []string) *fakedsp.Server {
	t.Helper()
	dsp := newDSP(t)
	q := `"` + id + `"`
	dsp.SetError(q+" get wallState 0",
		"INVALID_PARAMETER Index out of range:wallId min:1 max:"+itoa(walls)+" received:0")
	dsp.SetError(q+" get roomLabel 0",
		"INVALID_PARAMETER Index out of range:channelIdx min:1 max:"+itoa(rooms)+" received:0")
	for i, name := range names {
		dsp.SetValue(q+" get roomLabel "+itoa(i+1), `"`+name+`"`)
	}
	return dsp
}

func TestRoomCombinerDiscoversCountsFromErrors(t *testing.T) {
	names := []string{"Audytorium", "Korytarz", "Lazienka"}
	dsp := roomCombinerDSP(t, "RoomCombiner1", 3, 3, names)
	dsp.SetSubscriptionValue("wallState", "1", "true")
	dsp.SetSubscriptionValue("wallState", "2", "false")
	dsp.SetSubscriptionValue("wallState", "3", "true")
	dsp.SetOK(`"RoomCombiner1" set wallState 2 true`)

	pub, _ := runBlock(t, dsp, "RoomCombiner", "RoomCombiner1")

	pub.waitForValue(t, "walls/count", "3")
	pub.waitForValue(t, "rooms/count", "3")
	for i, name := range names {
		pub.waitForValue(t, "rooms/"+itoa(i+1)+"/name", name)
	}

	pub.waitForValue(t, "walls/1/state", "true")
	pub.waitForValue(t, "walls/2/state", "false")
	pub.waitForValue(t, "walls/3/state", "true")

	// Room names are read-only.
	if pub.hasHandler("rooms/1/name/set") {
		t.Error("room names must not be settable")
	}

	pub.send(t, "walls/2/state/set", "ON")
	waitForCommand(t, dsp, `"RoomCombiner1" set wallState 2 true`)

	dsp.PublishUpdate("wallState", "1", "false")
	pub.waitForValue(t, "walls/1/state", "false")
}

func TestParseBool(t *testing.T) {
	for _, in := range []string{"true", "TRUE", "1", "on", "ON", "yes", "y"} {
		got, err := blocks.ParseBool(in)
		if err != nil || !got {
			t.Errorf("ParseBool(%q) = %v, %v; want true", in, got, err)
		}
	}
	for _, in := range []string{"false", "FALSE", "0", "off", "no", "n"} {
		got, err := blocks.ParseBool(in)
		if err != nil || got {
			t.Errorf("ParseBool(%q) = %v, %v; want false", in, got, err)
		}
	}
	if _, err := blocks.ParseBool("maybe"); err == nil {
		t.Error("ParseBool(\"maybe\") should fail")
	}
}

func waitForCommand(t *testing.T, dsp *fakedsp.Server, command string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if dsp.Received(command) {
			return
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatalf("device never received %q; got %v", command, dsp.Commands())
		}
	}
}
