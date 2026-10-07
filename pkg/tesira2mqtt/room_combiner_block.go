package tesira2mqtt

import (
	"errors"
	"fmt"
	"log"
	"strconv"
)

type RoomCombinerBlock struct {
	Name             string
	Walls            []bool
	Groups           []int64
	SourceSelections []int64
}

func (b *RoomCombinerBlock) RunTTPDiscovery(c *TTPClient, blockName string) error {
	b.Name = blockName

	for wall := 1; ; wall++ {
		resp, err := c.RunCommand(fmt.Sprintf("%v get wallState %d", b.Name, wall))
		if err != nil {
			if wall == 1 {
				return fmt.Errorf("failed to get wallState for block %v: %v", blockName, err)
			}
			var ttpErr *TTPError
			if errors.As(err, &ttpErr) {
				break
			}
			return fmt.Errorf("failed to get wallState for block %v, wall %d: %v", blockName, wall, err)
		}

		state, ok := responseBoolValue(resp)
		if !ok {
			return fmt.Errorf("failed to convert wallState to bool for block %v, wall %d", blockName, wall)
		}
		b.Walls = append(b.Walls, state)
	}

	for room := 1; ; room++ {
		resp, err := c.RunCommand(fmt.Sprintf("%v get group %d", b.Name, room))
		if err != nil {
			if room == 1 {
				return fmt.Errorf("failed to get group for block %v: %v", blockName, err)
			}
			var ttpErr *TTPError
			if errors.As(err, &ttpErr) {
				break
			}
			return fmt.Errorf("failed to get group for block %v, room %d: %v", blockName, room, err)
		}

		group, ok := responseIntValue(resp)
		if !ok {
			return fmt.Errorf("failed to convert group to integer for block %v, room %d", blockName, room)
		}
		b.Groups = append(b.Groups, group)

		resp, err = c.RunCommand(fmt.Sprintf("%v get sourceSelection %d", b.Name, room))
		if err != nil {
			return fmt.Errorf("failed to get sourceSelection for block %v, room %d: %v", blockName, room, err)
		}
		source, ok := responseIntValue(resp)
		if !ok {
			return fmt.Errorf("failed to convert sourceSelection to integer for block %v, room %d", blockName, room)
		}
		b.SourceSelections = append(b.SourceSelections, source)
	}

	for wall := range b.Walls {
		msg := fmt.Sprintf("%v subscribe wallState %d T2M_%v_WALL_%d %v", b.Name, wall+1, b.Name, wall+1, BlockSubscriptionInterval)
		log.Printf("Subscribing to wallState for block %v, wall %d: %v", b.Name, wall+1, msg)
		_, err := c.RunCommand(msg)
		if err != nil {
			// Due to a known bug in current tesira firmware, we ignore wall subscription error for now
			log.Printf("failed to subscribe to wallState for block %v, wall %d: %v", blockName, wall+1, err)
			//return log.Errorf("failed to subscribe to wallState for block %v, wall %d: %v", blockName, wall+1, err)
		}
	}
	for room := range b.Groups {
		_, err := c.RunCommand(fmt.Sprintf("%v subscribe group %d T2M_%v_ROOM_%d_GROUP %v", b.Name, room+1, b.Name, room+1, BlockSubscriptionInterval))
		if err != nil {
			return fmt.Errorf("failed to subscribe to group for block %v, room %d: %v", blockName, room+1, err)
		}
		_, err = c.RunCommand(fmt.Sprintf("%v subscribe sourceSelection %d T2M_%v_ROOM_%d_SOURCE %v", b.Name, room+1, b.Name, room+1, BlockSubscriptionInterval))
		if err != nil {
			return fmt.Errorf("failed to subscribe to sourceSelection for block %v, room %d: %v", blockName, room+1, err)
		}
	}

	return nil
}

func (b *RoomCombinerBlock) HandleTTPSubscriptionData(_ *TTPClient, data *TTPSubscriptionData) (bool, error) {
	for wall := range b.Walls {
		if data.PublishToken != fmt.Sprintf("T2M_%v_WALL_%d", b.Name, wall+1) {
			continue
		}

		state, ok := data.Data.(bool)
		if !ok {
			return false, fmt.Errorf("failed to convert wallState to bool for block %v, wall %d", b.Name, wall+1)
		}
		b.Walls[wall] = state
		return true, nil
	}
	for room := range b.Groups {
		if data.PublishToken == fmt.Sprintf("T2M_%v_ROOM_%d_GROUP", b.Name, room+1) {
			group, ok := data.Data.(int64)
			if !ok {
				return false, fmt.Errorf("failed to convert group to integer for block %v, room %d", b.Name, room+1)
			}
			b.Groups[room] = group
			return true, nil
		}
		if data.PublishToken == fmt.Sprintf("T2M_%v_ROOM_%d_SOURCE", b.Name, room+1) {
			source, ok := data.Data.(int64)
			if !ok {
				return false, fmt.Errorf("failed to convert sourceSelection to integer for block %v, room %d", b.Name, room+1)
			}
			b.SourceSelections[room] = source
			return true, nil
		}
	}
	return false, nil
}

func (b *RoomCombinerBlock) PublishStateToMQTT(c *TTPClient) error {
	c.mqttClient.Publish(fmt.Sprintf("%v%v/type", config.MQTT.Prefix, b.Name), 0, true, "RoomCombiner")
	c.mqttClient.Publish(fmt.Sprintf("%v%v/num_walls", config.MQTT.Prefix, b.Name), 0, true, fmt.Sprintf("%v", len(b.Walls)))
	c.mqttClient.Publish(fmt.Sprintf("%v%v/num_rooms", config.MQTT.Prefix, b.Name), 0, true, fmt.Sprintf("%v", len(b.Groups)))
	for wall, state := range b.Walls {
		c.mqttClient.Publish(fmt.Sprintf("%v%v/wall/%d", config.MQTT.Prefix, b.Name, wall+1), 0, true, fmt.Sprintf("%v", state))
	}
	for room, group := range b.Groups {
		c.mqttClient.Publish(fmt.Sprintf("%v%v/room/%d/group", config.MQTT.Prefix, b.Name, room+1), 0, true, fmt.Sprintf("%v", group))
		c.mqttClient.Publish(fmt.Sprintf("%v%v/room/%d/source", config.MQTT.Prefix, b.Name, room+1), 0, true, fmt.Sprintf("%v", b.SourceSelections[room]))
	}
	return nil
}

func (b *RoomCombinerBlock) HandleMQTTMessage(c *TTPClient, topic string, payload string) error {
	for wall := range b.Walls {
		if topic != fmt.Sprintf("%v%v/wall/%d/set", config.MQTT.Prefix, b.Name, wall+1) {
			continue
		}

		state, err := strconv.ParseBool(payload)
		if err != nil {
			return fmt.Errorf("failed to parse wall state from payload: %v", err)
		}
		_, err = c.RunCommand(fmt.Sprintf("%v set wallState %d %v", b.Name, wall+1, state))
		if err != nil {
			return fmt.Errorf("failed to set wall state for block %v, wall %d: %v", b.Name, wall+1, err)
		}
		return nil
	}
	for room := range b.Groups {
		if topic == fmt.Sprintf("%v%v/room/%d/group/set", config.MQTT.Prefix, b.Name, room+1) {
			group, err := strconv.ParseInt(payload, 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse room group from payload: %v", err)
			}
			_, err = c.RunCommand(fmt.Sprintf("%v set group %d %d", b.Name, room+1, group))
			if err != nil {
				return fmt.Errorf("failed to set group for block %v, room %d: %v", b.Name, room+1, err)
			}
			return nil
		}
		if topic == fmt.Sprintf("%v%v/room/%d/source/set", config.MQTT.Prefix, b.Name, room+1) {
			source, err := strconv.ParseInt(payload, 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse source selection from payload: %v", err)
			}
			_, err = c.RunCommand(fmt.Sprintf("%v set sourceSelection %d %d", b.Name, room+1, source))
			if err != nil {
				return fmt.Errorf("failed to set source selection for block %v, room %d: %v", b.Name, room+1, err)
			}
			return nil
		}
	}
	return nil
}

func responseBoolValue(resp *TTPResponse) (bool, bool) {
	if resp == nil {
		return false, false
	}
	value, ok := resp.Value.(map[string]any)
	if !ok {
		return false, false
	}
	state, ok := value["value"].(bool)
	return state, ok
}

func responseIntValue(resp *TTPResponse) (int64, bool) {
	if resp == nil {
		return 0, false
	}
	value, ok := resp.Value.(map[string]any)
	if !ok {
		return 0, false
	}
	number, ok := value["value"].(int64)
	return number, ok
}
