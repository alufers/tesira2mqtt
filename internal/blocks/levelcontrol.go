package blocks

import (
	"context"
	"fmt"
	"sync"

	"github.com/alufers/tesira2mqtt/internal/ttp"
)

func init() {
	Register("LevelControl", func(d Deps) Block { return &levelControl{d: d} })
}

// levelControl bridges a Tesira Level Control block.
//
// A ganged block is one knob driving every channel together - a stereo pair,
// typically - so it gets a single set of topics at the block root and a write
// there fans out to all channels. A block that is not ganged exposes its
// channels individually under channels/<n>/, except when it has just one
// channel, in which case the root topics are the whole block.
type levelControl struct {
	d Deps

	numChannels int
	ganged      bool
	channels    []*levelChannel
}

type levelChannel struct {
	index int
	label string
	min   float64
	max   float64

	mu       sync.Mutex
	level    float64
	hasLevel bool
	muted    bool
	hasMute  bool
}

// rootTopics reports whether the block's controls live at its root rather than
// under channels/<n>/.
func (b *levelControl) rootTopics() bool { return b.ganged || b.numChannels == 1 }

func (b *levelControl) Discover(ctx context.Context) error {
	res, err := b.d.Client.Get(ctx, b.d.ID, "numChannels")
	if err != nil {
		return fmt.Errorf("numChannels: %w", err)
	}
	if b.numChannels, err = res.Int(); err != nil {
		return fmt.Errorf("numChannels: %w", err)
	}
	if b.numChannels < 1 {
		return fmt.Errorf("block reports %d channels", b.numChannels)
	}

	// Not every level-bearing block is ganged-capable; treat a rejection as
	// "not ganged" rather than failing the whole block.
	if res, err := b.d.Client.Get(ctx, b.d.ID, "ganged"); err == nil {
		b.ganged, _ = res.Bool()
	}

	for i := 1; i <= b.numChannels; i++ {
		ch := &levelChannel{index: i}

		if res, err := b.d.Client.Get(ctx, b.d.ID, "label", i); err == nil {
			ch.label, _ = res.Str()
		}

		res, err := b.d.Client.Get(ctx, b.d.ID, "minLevel", i)
		if err != nil {
			return fmt.Errorf("minLevel %d: %w", i, err)
		}
		if ch.min, err = res.Float(); err != nil {
			return fmt.Errorf("minLevel %d: %w", i, err)
		}

		res, err = b.d.Client.Get(ctx, b.d.ID, "maxLevel", i)
		if err != nil {
			return fmt.Errorf("maxLevel %d: %w", i, err)
		}
		if ch.max, err = res.Float(); err != nil {
			return fmt.Errorf("maxLevel %d: %w", i, err)
		}

		b.channels = append(b.channels, ch)
	}

	b.registerCommands()

	b.d.Log.Info("level control discovered", "block", b.d.ID,
		"channels", b.numChannels, "ganged", b.ganged)
	return nil
}

// registerCommands wires up the /set topics.
func (b *levelControl) registerCommands() {
	if b.rootTopics() {
		// A write at the root drives every channel, which is what "ganged"
		// means on the device and what a single-channel block trivially is.
		targets := b.channels
		b.d.Pub.Subscribe("level_dB/set", b.setLevelHandler(targets))
		b.d.Pub.Subscribe("level_percent/set", b.setPercentHandler(targets))
		b.d.Pub.Subscribe("mute/set", b.setMuteHandler(targets))
		return
	}
	for _, ch := range b.channels {
		one := []*levelChannel{ch}
		base := fmt.Sprintf("channels/%d/", ch.index)
		b.d.Pub.Subscribe(base+"level_dB/set", b.setLevelHandler(one))
		b.d.Pub.Subscribe(base+"level_percent/set", b.setPercentHandler(one))
		b.d.Pub.Subscribe(base+"mute/set", b.setMuteHandler(one))
	}
}

func (b *levelControl) setLevelHandler(targets []*levelChannel) func(string) {
	return func(payload string) {
		v, err := ParseFloat(payload)
		if err != nil {
			b.d.Log.Warn("bad level payload", "block", b.d.ID, "payload", payload, "err", err)
			return
		}
		b.applyLevel(targets, func(ch *levelChannel) float64 { return Clamp(v, ch.min, ch.max) })
	}
}

func (b *levelControl) setPercentHandler(targets []*levelChannel) func(string) {
	return func(payload string) {
		pct, err := ParseFloat(payload)
		if err != nil {
			b.d.Log.Warn("bad percent payload", "block", b.d.ID, "payload", payload, "err", err)
			return
		}
		pct = Clamp(pct, 0, 100)
		b.applyLevel(targets, func(ch *levelChannel) float64 {
			return ch.min + pct/100*(ch.max-ch.min)
		})
	}
}

// applyLevel writes a level to each target channel. The resulting value is not
// published here: the device's subscription reports what it actually accepted,
// which keeps MQTT honest when a level is clamped or refused.
func (b *levelControl) applyLevel(targets []*levelChannel, value func(*levelChannel) float64) {
	ctx := context.Background()
	for _, ch := range targets {
		v := value(ch)
		if _, err := b.d.Client.Set(ctx, b.d.ID, "level", ttp.FormatFloat(v), ch.index); err != nil {
			b.d.Log.Warn("set level failed", "block", b.d.ID, "channel", ch.index, "value", v, "err", err)
		}
	}
}

func (b *levelControl) setMuteHandler(targets []*levelChannel) func(string) {
	return func(payload string) {
		muted, err := ParseBool(payload)
		if err != nil {
			b.d.Log.Warn("bad mute payload", "block", b.d.ID, "payload", payload, "err", err)
			return
		}
		ctx := context.Background()
		for _, ch := range targets {
			if _, err := b.d.Client.Set(ctx, b.d.ID, "mute", FormatBool(muted), ch.index); err != nil {
				b.d.Log.Warn("set mute failed", "block", b.d.ID, "channel", ch.index, "err", err)
			}
		}
	}
}

func (b *levelControl) Sync(ctx context.Context) error {
	b.publishStatic()

	// "levels" and "mutes" publish one array covering every channel, so a
	// single block-wide subscription each is enough.
	if err := b.d.Client.Subscribe(ctx, b.d.ID, "levels", nil, b.onLevels); err != nil {
		return err
	}
	if err := b.d.Client.Subscribe(ctx, b.d.ID, "mutes", nil, b.onMutes); err != nil {
		return err
	}
	return nil
}

func (b *levelControl) publishStatic() {
	b.d.Pub.Publish("channels/count", FormatInt(b.numChannels))
	b.d.Pub.Publish("ganged", FormatBool(b.ganged))

	for _, ch := range b.channels {
		if b.rootTopics() && ch.index != 1 {
			continue
		}
		p := b.channelPrefix(ch)
		b.d.Pub.Publish(p+"min_level_dB", FormatDB(ch.min))
		b.d.Pub.Publish(p+"max_level_dB", FormatDB(ch.max))
		if ch.label != "" {
			b.d.Pub.Publish(p+"label", ch.label)
		}
		b.publishChannelState(ch)
	}
}

// channelPrefix is the relative topic prefix a channel's leaves hang off.
func (b *levelControl) channelPrefix(ch *levelChannel) string {
	if b.rootTopics() {
		return ""
	}
	return fmt.Sprintf("channels/%d/", ch.index)
}

func (b *levelControl) onLevels(res *ttp.Response) {
	levels, err := res.Floats()
	if err != nil {
		b.d.Log.Warn("unexpected levels update", "block", b.d.ID, "raw", res.Raw, "err", err)
		return
	}
	for i, level := range levels {
		ch := b.channel(i + 1)
		if ch == nil {
			b.d.Log.Debug("levels update for unknown channel", "block", b.d.ID, "channel", i+1)
			continue
		}
		ch.mu.Lock()
		ch.level, ch.hasLevel = level, true
		ch.mu.Unlock()
		b.publishChannelState(ch)
	}
}

func (b *levelControl) onMutes(res *ttp.Response) {
	mutes, err := res.Bools()
	if err != nil {
		b.d.Log.Warn("unexpected mutes update", "block", b.d.ID, "raw", res.Raw, "err", err)
		return
	}
	for i, muted := range mutes {
		ch := b.channel(i + 1)
		if ch == nil {
			b.d.Log.Debug("mutes update for unknown channel", "block", b.d.ID, "channel", i+1)
			continue
		}
		ch.mu.Lock()
		ch.muted, ch.hasMute = muted, true
		ch.mu.Unlock()
		b.publishChannelState(ch)
	}
}

// publishChannelState emits a channel's live values. A ganged block publishes
// only channel 1, since its root topics represent the whole block.
func (b *levelControl) publishChannelState(ch *levelChannel) {
	if b.rootTopics() && ch.index != 1 {
		return
	}
	p := b.channelPrefix(ch)

	ch.mu.Lock()
	level, hasLevel := ch.level, ch.hasLevel
	muted, hasMute := ch.muted, ch.hasMute
	ch.mu.Unlock()

	if hasLevel {
		b.d.Pub.Publish(p+"level_dB", FormatDB(level))
		b.d.Pub.Publish(p+"level_percent", FormatPercent(percentOf(level, ch.min, ch.max)))
	}
	if hasMute {
		b.d.Pub.Publish(p+"mute", FormatBool(muted))
	}
}

func (b *levelControl) channel(index int) *levelChannel {
	if index < 1 || index > len(b.channels) {
		return nil
	}
	return b.channels[index-1]
}

// percentOf maps a level onto its block's configured min/max span.
func percentOf(level, min, max float64) float64 {
	if max <= min {
		return 0
	}
	return Clamp((level-min)/(max-min)*100, 0, 100)
}

var _ Block = (*levelControl)(nil)
