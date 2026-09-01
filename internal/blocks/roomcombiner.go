package blocks

import (
	"context"
	"fmt"
	"sync"

	"github.com/alufers/tesira2mqtt/internal/ttp"
)

func init() {
	Register("RoomCombiner", func(d Deps) Block { return &roomCombiner{d: d} })
}

// roomCombiner bridges a Tesira Room Combiner block.
//
// The block publishes no wall or room count attribute, so both are read out of
// the bounds the device reports when asked for the deliberately invalid index
// 0. Room labels are not subscribable, so they are read once at discovery.
type roomCombiner struct {
	d Deps

	numWalls int
	numRooms int

	mu         sync.Mutex
	wallStates map[int]bool
	roomLabels map[int]string
}

func (b *roomCombiner) Discover(ctx context.Context) error {
	wallRange, err := b.d.Client.IndexRange(ctx, b.d.ID, "wallState")
	if err != nil {
		return fmt.Errorf("wall count: %w", err)
	}
	b.numWalls = wallRange.Max

	roomRange, err := b.d.Client.IndexRange(ctx, b.d.ID, "roomLabel")
	if err != nil {
		return fmt.Errorf("room count: %w", err)
	}
	b.numRooms = roomRange.Max

	b.wallStates = make(map[int]bool, b.numWalls)
	b.roomLabels = make(map[int]string, b.numRooms)

	for i := 1; i <= b.numRooms; i++ {
		res, err := b.d.Client.Get(ctx, b.d.ID, "roomLabel", i)
		if err != nil {
			return fmt.Errorf("roomLabel %d: %w", i, err)
		}
		label, err := res.Str()
		if err != nil {
			return fmt.Errorf("roomLabel %d: %w", i, err)
		}
		b.mu.Lock()
		b.roomLabels[i] = label
		b.mu.Unlock()
	}

	for wall := 1; wall <= b.numWalls; wall++ {
		b.d.Pub.Subscribe(fmt.Sprintf("walls/%d/state/set", wall), b.setWallHandler(wall))
	}

	b.d.Log.Info("room combiner discovered", "block", b.d.ID,
		"walls", b.numWalls, "rooms", b.numRooms)
	return nil
}

func (b *roomCombiner) setWallHandler(wall int) func(string) {
	return func(payload string) {
		closed, err := ParseBool(payload)
		if err != nil {
			b.d.Log.Warn("bad wall state payload", "block", b.d.ID, "wall", wall,
				"payload", payload, "err", err)
			return
		}
		if _, err := b.d.Client.Set(context.Background(), b.d.ID, "wallState", FormatBool(closed), wall); err != nil {
			b.d.Log.Warn("set wall state failed", "block", b.d.ID, "wall", wall, "err", err)
		}
	}
}

func (b *roomCombiner) Sync(ctx context.Context) error {
	b.publishStatic()

	// wallState is indexed and rejects an index-less subscription, so each wall
	// is subscribed on its own.
	for wall := 1; wall <= b.numWalls; wall++ {
		wall := wall
		if err := b.d.Client.Subscribe(ctx, b.d.ID, "wallState", &wall, func(res *ttp.Response) {
			b.onWallState(wall, res)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (b *roomCombiner) publishStatic() {
	b.d.Pub.Publish("walls/count", FormatInt(b.numWalls))
	b.d.Pub.Publish("rooms/count", FormatInt(b.numRooms))

	b.mu.Lock()
	labels := make(map[int]string, len(b.roomLabels))
	for i, l := range b.roomLabels {
		labels[i] = l
	}
	states := make(map[int]bool, len(b.wallStates))
	for i, s := range b.wallStates {
		states[i] = s
	}
	b.mu.Unlock()

	for room, label := range labels {
		b.d.Pub.Publish(fmt.Sprintf("rooms/%d/name", room), label)
	}
	for wall, closed := range states {
		b.d.Pub.Publish(fmt.Sprintf("walls/%d/state", wall), FormatBool(closed))
	}
}

func (b *roomCombiner) onWallState(wall int, res *ttp.Response) {
	closed, err := res.Bool()
	if err != nil {
		b.d.Log.Warn("unexpected wall state update", "block", b.d.ID, "wall", wall,
			"raw", res.Raw, "err", err)
		return
	}
	b.mu.Lock()
	b.wallStates[wall] = closed
	b.mu.Unlock()

	b.d.Pub.Publish(fmt.Sprintf("walls/%d/state", wall), FormatBool(closed))
}

var _ Block = (*roomCombiner)(nil)
