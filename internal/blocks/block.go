// Package blocks maps Tesira DSP blocks onto MQTT topics.
//
// Adding support for a new block type means adding one file here that
// implements Block and registers a factory in its init; nothing else in the
// tree needs to change.
package blocks

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/alufers/tesira2mqtt/internal/tesira"
)

// Publisher is the MQTT surface a block is given, rooted at that block's topic.
type Publisher interface {
	// Publish sends a retained state update on a topic relative to the block.
	Publish(rel, payload string)
	// Subscribe registers a handler for a relative command topic. Handlers
	// survive broker reconnects.
	Subscribe(rel string, handler func(payload string))
}

// Deps is everything a block implementation needs.
type Deps struct {
	Log    *slog.Logger
	Client *tesira.Client
	ID     string // Tesira instance tag
	Type   string // Tesira block type
	Pub    Publisher
}

// Block is one DSP block bridged to MQTT.
type Block interface {
	// Discover reads the block's static layout - channel counts, labels,
	// level limits - and registers its MQTT command topics. It runs once, on
	// the first successful connection.
	Discover(ctx context.Context) error

	// Sync (re)establishes the block's Tesira subscriptions and republishes
	// everything it knows. It runs on every connection, including reconnects.
	Sync(ctx context.Context) error
}

// Factory builds a block of a particular Tesira type.
type Factory func(Deps) Block

var registry = map[string]Factory{}

// Register adds a block type to the registry. Call it from an init function.
func Register(blockType string, f Factory) {
	if _, dup := registry[blockType]; dup {
		panic(fmt.Sprintf("blocks: %q registered twice", blockType))
	}
	registry[blockType] = f
}

// New builds a block for a Tesira block type, reporting whether the type is
// supported.
func New(blockType string, d Deps) (Block, bool) {
	f, ok := registry[blockType]
	if !ok {
		return nil, false
	}
	return f(d), true
}

// SupportedTypes lists the registered block types, sorted.
func SupportedTypes() []string {
	out := make([]string, 0, len(registry))
	for t := range registry {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
