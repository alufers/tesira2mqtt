package tesira

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/alufers/tesira2mqtt/internal/ttp"
)

// SubscriptionHandler receives an attribute update. It runs on the reader
// goroutine and must not issue commands.
type SubscriptionHandler func(res *ttp.Response)

type subscription struct {
	key     string
	token   string
	blockID string
	attr    string
	index   *int
	handler SubscriptionHandler
}

// command renders the TTP subscribe command for this subscription.
func (s *subscription) command() string {
	if s.index == nil {
		return fmt.Sprintf("%s subscribe %s %q", quoteTag(s.blockID), s.attr, s.token)
	}
	return fmt.Sprintf("%s subscribe %s %d %q", quoteTag(s.blockID), s.attr, *s.index, s.token)
}

// alreadySubscribed is the device's answer when a subscription with the same
// token is already live. It means the subscription is healthy, so it is treated
// as success rather than as an error.
const alreadySubscribed = "ALREADY_SUBSCRIBED"

// isAlreadySubscribed reports whether err is the device declining a duplicate
// subscription.
func isAlreadySubscribed(err error) bool {
	cerr, ok := AsCommandError(err)
	return ok && strings.Contains(cerr.Message, alreadySubscribed)
}

// Subscribe registers an attribute subscription and asks the device to start
// publishing it. Pass a nil index for block-wide attributes such as "levels"
// and "mutes", which publish an array covering every channel; indexed
// attributes such as "wallState" must be subscribed one index at a time.
//
// Subscribing again for the same block, attribute and index reuses the same
// publish token and replaces the handler, which makes it safe to call on every
// reconnect and on the periodic refresh.
func (c *Client) Subscribe(ctx context.Context, blockID, attr string, index *int, handler SubscriptionHandler) error {
	sub := c.registerSubscription(blockID, attr, index, handler)
	if _, err := c.Command(ctx, sub.command()); err != nil {
		if isAlreadySubscribed(err) {
			return nil
		}
		return fmt.Errorf("subscribe %s %s: %w", blockID, attr, err)
	}
	return nil
}

func (c *Client) registerSubscription(blockID, attr string, index *int, handler SubscriptionHandler) *subscription {
	idxLabel := "ALL"
	if index != nil {
		idxLabel = strconv.Itoa(*index)
	}
	key := blockID + "\x00" + attr + "\x00" + idxLabel

	c.subMu.Lock()
	defer c.subMu.Unlock()

	if existing, ok := c.subs[key]; ok {
		existing.handler = handler
		return existing
	}

	c.subSeq++
	sub := &subscription{
		key:     key,
		token:   fmt.Sprintf("S%d_%s_%s_%s", c.subSeq, attr, idxLabel, sanitizeToken(blockID)),
		blockID: blockID,
		attr:    attr,
		index:   index,
		handler: handler,
	}
	c.subs[key] = sub
	c.subsByTok[sub.token] = sub
	return sub
}

// dispatchPublish routes an update to its handler. Routing is a map lookup on
// the publish token rather than an attempt to parse the token's parts, so block
// names containing underscores or spaces cannot misroute an update.
func (c *Client) dispatchPublish(res *ttp.Response) {
	c.subMu.Lock()
	sub := c.subsByTok[res.PublishToken]
	c.subMu.Unlock()

	if sub == nil {
		c.log.Debug("update for unknown subscription", "token", res.PublishToken)
		return
	}

	c.subMu.Lock()
	handler := sub.handler
	c.subMu.Unlock()

	if handler != nil {
		handler(res)
	}
}

// Resubscribe reissues every registered subscription as a liveness check. A
// healthy subscription is declined with ALREADY_SUBSCRIBED; a "+OK" means the
// device had silently dropped it - a reprogrammed DSP, typically - and has just
// re-established it, publishing the current value in the process.
func (c *Client) Resubscribe(ctx context.Context) error {
	c.subMu.Lock()
	subs := make([]*subscription, 0, len(c.subs))
	for _, s := range c.subs {
		subs = append(subs, s)
	}
	c.subMu.Unlock()

	var firstErr error
	for _, s := range subs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, err := c.Command(ctx, s.command())
		switch {
		case err == nil:
			c.log.Info("subscription had lapsed and was restored",
				"block", s.blockID, "attr", s.attr)
		case isAlreadySubscribed(err):
			// Healthy.
		default:
			c.log.Debug("resubscribe failed", "block", s.blockID, "attr", s.attr, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (c *Client) resubscribeLoop(ctx context.Context) {
	ticker := time.NewTicker(c.opts.ResubscribeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Resubscribe(ctx); err != nil && ctx.Err() == nil {
				c.log.Debug("periodic resubscribe incomplete", "err", err)
			}
		}
	}
}

// quoteTag wraps an instance tag in quotes so names containing spaces work.
func quoteTag(tag string) string {
	return `"` + strings.ReplaceAll(tag, `"`, `\"`) + `"`
}

// sanitizeToken makes a block name safe to embed in a publish token. Tokens are
// only ever matched by exact lookup, so this is purely for readable logs.
func sanitizeToken(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}
