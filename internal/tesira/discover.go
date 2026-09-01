package tesira

import (
	"context"
	"fmt"
	"strings"

	"github.com/alufers/tesira2mqtt/internal/ttp"
)

// BlockInfo identifies one DSP block on the device.
type BlockInfo struct {
	ID   string // instance tag, e.g. "level_anc"
	Type string // block type, e.g. "LevelControl"
}

// blockTypeProbe is an attribute no block implements. Asking for it makes the
// device name the interface that rejected it, which is the only way to learn a
// block's type - there is no "get type" command.
const blockTypeProbe = "BLOCKTYPE"

// interfaceSuffix is what the device appends to the block type in that error,
// as in "LevelControlInterface::Attributes".
const interfaceSuffix = "Interface::Attributes"

// DiscoverBlocks resolves the type of every DSP block on the device.
func (c *Client) DiscoverBlocks(ctx context.Context) ([]BlockInfo, error) {
	aliases := c.Info().Aliases
	blocks := make([]BlockInfo, 0, len(aliases))

	for _, alias := range aliases {
		if strings.EqualFold(alias, "DEVICE") {
			continue // the device handle itself, not a DSP block
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		blockType, err := c.BlockType(ctx, alias)
		if err != nil {
			c.log.Debug("skipping block with undetermined type", "block", alias, "err", err)
			continue
		}
		blocks = append(blocks, BlockInfo{ID: alias, Type: blockType})
	}

	return blocks, nil
}

// BlockType returns the type of a single block.
func (c *Client) BlockType(ctx context.Context, blockID string) (string, error) {
	_, err := c.Commandf(ctx, "%s get %s", quoteTag(blockID), blockTypeProbe)
	if err == nil {
		// A block that answers this is not one we can identify.
		return "", fmt.Errorf("block %q unexpectedly accepted the type probe", blockID)
	}

	cerr, ok := AsCommandError(err)
	if !ok {
		return "", err
	}
	idx := strings.Index(cerr.Message, interfaceSuffix)
	if idx < 0 {
		return "", fmt.Errorf("block %q has no attribute interface: %s", blockID, cerr.Message)
	}

	// "'BLOCKTYPE' is not supported by LevelControlInterface::Attributes"
	// -> take the last word before the suffix.
	head := strings.TrimSpace(cerr.Message[:idx])
	fields := strings.Fields(head)
	if len(fields) == 0 {
		return "", fmt.Errorf("block %q reported an empty type: %s", blockID, cerr.Message)
	}
	return fields[len(fields)-1], nil
}

// maxIndexProbeCeiling bounds the fallback scan for an attribute's index range.
const maxIndexProbeCeiling = 64

// IndexRange discovers the valid index range of an indexed attribute. Blocks
// such as Room Combiner publish no count attribute, so the range is read out of
// the error the device returns for the deliberately invalid index 0.
func (c *Client) IndexRange(ctx context.Context, blockID, attr string) (ttp.IndexRange, error) {
	_, err := c.Commandf(ctx, "%s get %s 0", quoteTag(blockID), attr)
	if cerr, ok := AsCommandError(err); ok {
		if r, ok := ttp.ParseIndexRange(cerr.Message); ok {
			return r, nil
		}
	} else if err != nil {
		return ttp.IndexRange{}, err
	}

	// The device did not report bounds in the way we expect, so count upwards
	// until an index is rejected.
	c.log.Debug("index bounds not reported, probing upwards", "block", blockID, "attr", attr)
	max := 0
	for i := 1; i <= maxIndexProbeCeiling; i++ {
		if _, err := c.Commandf(ctx, "%s get %s %d", quoteTag(blockID), attr, i); err != nil {
			if _, ok := AsCommandError(err); ok {
				break
			}
			return ttp.IndexRange{}, err
		}
		max = i
	}
	if max == 0 {
		return ttp.IndexRange{}, fmt.Errorf("block %q has no valid index for %q", blockID, attr)
	}
	return ttp.IndexRange{Name: attr, Min: 1, Max: max}, nil
}

// Get reads an attribute, with an optional index list.
func (c *Client) Get(ctx context.Context, blockID, attr string, indexes ...int) (*ttp.Response, error) {
	return c.Command(ctx, buildCommand(blockID, "get", attr, indexes, ""))
}

// Set writes an attribute. value must already be in TTP form: a number, "true"
// or "false", or a quoted string.
func (c *Client) Set(ctx context.Context, blockID, attr string, value string, indexes ...int) (*ttp.Response, error) {
	return c.Command(ctx, buildCommand(blockID, "set", attr, indexes, value))
}

func buildCommand(blockID, verb, attr string, indexes []int, value string) string {
	var b strings.Builder
	b.WriteString(quoteTag(blockID))
	b.WriteString(" ")
	b.WriteString(verb)
	b.WriteString(" ")
	b.WriteString(attr)
	for _, i := range indexes {
		fmt.Fprintf(&b, " %d", i)
	}
	if value != "" {
		b.WriteString(" ")
		b.WriteString(value)
	}
	return b.String()
}
