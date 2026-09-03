package tesira2mqtt

import (
	"fmt"
	"log"
	"strconv"
)

type LevelControlBlock struct {
	Name   string
	Levels []float64
	Mins   []float64
	Maxes  []float64
	Mutes  []bool
	Ganged bool
}

func (b *LevelControlBlock) RunTTPDiscovery(c *TTPClient, blockName string) error {
	b.Name = blockName
	currentLevelsResp, err := c.RunCommand(fmt.Sprintf("%v get levels", b.Name))
	if err != nil {
		return fmt.Errorf("failed to get levels for block %v: %v", blockName, err)
	}

	var ok bool
	b.Levels, ok = toFloat64Slice(currentLevelsResp.Value.(map[string]any)["value"])

	if !ok {
		return fmt.Errorf("failed to convert levels to float64 slice for block %v", blockName)
	}

	b.Mins = make([]float64, len(b.Levels))
	b.Maxes = make([]float64, len(b.Levels))
	for idx := range b.Levels {
		minResp, err := c.RunCommand(fmt.Sprintf("%v get minLevel %d", b.Name, idx+1))
		if err != nil {
			return fmt.Errorf("failed to get minLevel for block %v, level %d: %v", blockName, idx+1, err)
		}
		b.Mins[idx], ok = minResp.Value.(map[string]any)["value"].(float64)
		if !ok {
			return fmt.Errorf("failed to convert minLevel to float64 for block %v, level %d", blockName, idx+1)
		}

		maxResp, err := c.RunCommand(fmt.Sprintf("%v get maxLevel %d", b.Name, idx+1))
		if err != nil {
			return fmt.Errorf("failed to get maxLevel for block %v, level %d: %v", blockName, idx+1, err)
		}
		b.Maxes[idx], ok = maxResp.Value.(map[string]any)["value"].(float64)
		if !ok {
			return fmt.Errorf("failed to convert maxLevel to float64 for block %v, level %d", blockName, idx+1)
		}
	}

	channelsGangedResp, err := c.RunCommand(fmt.Sprintf("%v get ganged", b.Name))
	if err != nil {
		return fmt.Errorf("failed to get channelsGanged for block %v: %v", blockName, err)
	}
	b.Ganged, ok = channelsGangedResp.Value.(map[string]any)["value"].(bool)
	if !ok {
		return fmt.Errorf("failed to convert channelsGanged to bool for block %v", blockName)
	}

	mutesResp, err := c.RunCommand(fmt.Sprintf("%v get mutes", b.Name))
	if err != nil {
		return fmt.Errorf("failed to get mutes for block %v: %v", blockName, err)
	}
	b.Mutes, ok = toBoolSlice(mutesResp.Value.(map[string]any)["value"])
	if !ok {
		return fmt.Errorf("failed to convert mutes to bool slice for block %v", blockName)
	}

	// Subscribe to levels and mutes updates
	_, err = c.RunCommand(fmt.Sprintf("%v subscribe levels T2M_%v_LEVELS %v", b.Name, b.Name, BlockSubscriptionInterval))
	if err != nil {
		// Errors are ok here, since we might be subscribing again
		return fmt.Errorf("failed to subscribe to levels for block %v: %v", blockName, err)
	}

	// Subscribe to levels and mutes updates
	_, err = c.RunCommand(fmt.Sprintf("%v subscribe mutes T2M_%v_MUTES %v", b.Name, b.Name, BlockSubscriptionInterval))
	if err != nil {
		// Errors are ok here, since we might be subscribing again
		log.Printf("failed to subscribe to mutes for block %v: %v", blockName, err)
	}

	return nil
}

func (b *LevelControlBlock) HandleTTPSubscriptionData(c *TTPClient, data *TTPSubscriptionData) (bool, error) {
	if data.PublishToken == fmt.Sprintf("T2M_%v_LEVELS", b.Name) {
		levels, ok := toFloat64Slice(data.Data)
		if !ok {
			return false, fmt.Errorf("failed to convert levels to float64 slice for block %v", b.Name)
		}
		b.Levels = levels
		return true, nil
	} else if data.PublishToken == fmt.Sprintf("T2M_%v_MUTES", b.Name) {
		mutes, ok := toBoolSlice(data.Data)
		if !ok {
			return false, fmt.Errorf("failed to convert mutes to bool slice for block %v", b.Name)
		}
		b.Mutes = mutes
		return true, nil
	}
	return false, nil
}

func (b *LevelControlBlock) PublishStateToMQTT(c *TTPClient) error {
	c.mqttClient.Publish(fmt.Sprintf("%v%v/type", config.MQTT.Prefix, b.Name), 0, true, "LevelControl")
	c.mqttClient.Publish(fmt.Sprintf("%v%v/ganged", config.MQTT.Prefix, b.Name), 0, true, fmt.Sprintf("%v", b.Ganged))
	c.mqttClient.Publish(fmt.Sprintf("%v%v/num_channels", config.MQTT.Prefix, b.Name), 0, true, fmt.Sprintf("%v", len(b.Levels)))
	for idx, level := range b.Levels {
		c.mqttClient.Publish(fmt.Sprintf("%v%v/level/%d", config.MQTT.Prefix, b.Name, idx+1), 0, true, fmt.Sprintf("%v", level))
		c.mqttClient.Publish(fmt.Sprintf("%v%v/min/%d", config.MQTT.Prefix, b.Name, idx+1), 0, true, fmt.Sprintf("%v", b.Mins[idx]))
		c.mqttClient.Publish(fmt.Sprintf("%v%v/max/%d", config.MQTT.Prefix, b.Name, idx+1), 0, true, fmt.Sprintf("%v", b.Maxes[idx]))

		levelPercent := (level - b.Mins[idx]) / (b.Maxes[idx] - b.Mins[idx]) * 100.0
		c.mqttClient.Publish(fmt.Sprintf("%v%v/level_percent/%d", config.MQTT.Prefix, b.Name, idx+1), 0, true, fmt.Sprintf("%v", levelPercent))

		c.mqttClient.Publish(fmt.Sprintf("%v%v/mute/%d", config.MQTT.Prefix, b.Name, idx+1), 0, true, fmt.Sprintf("%v", b.Mutes[idx]))
	}
	return nil
}

func (b *LevelControlBlock) HandleMQTTMessage(c *TTPClient, topic string, payload string) error {

	for idx := range b.Levels {
		if topic == fmt.Sprintf("%v%v/level/%d/set", config.MQTT.Prefix, b.Name, idx+1) {
			level, err := strconv.ParseFloat(payload, 64)
			if err != nil {
				return fmt.Errorf("failed to parse level from payload: %v", err)
			}
			_, err = c.RunCommand(fmt.Sprintf("%v set level %d %v", b.Name, idx+1, level))
			if err != nil {
				return fmt.Errorf("failed to set level for block %v, level %d: %v", b.Name, idx+1, err)
			}
			return nil
		} else if topic == fmt.Sprintf("%v%v/mute/%d/set", config.MQTT.Prefix, b.Name, idx+1) {
			mute := payload == "true"
			_, err := c.RunCommand(fmt.Sprintf("%v set mute %d %v", b.Name, idx+1, mute))
			if err != nil {
				return fmt.Errorf("failed to set mute for block %v, level %d: %v", b.Name, idx+1, err)
			}
			return nil
		} else if topic == fmt.Sprintf("%v%v/level_percent/%d/set", config.MQTT.Prefix, b.Name, idx+1) {
			levelPercent, err := strconv.ParseFloat(payload, 64)
			if err != nil {
				return fmt.Errorf("failed to parse level percent from payload: %v", err)
			}
			level := b.Mins[idx] + (levelPercent/100.0)*(b.Maxes[idx]-b.Mins[idx])
			_, err = c.RunCommand(fmt.Sprintf("%v set level %d %v", b.Name, idx+1, level))
			if err != nil {
				return fmt.Errorf("failed to set level for block %v, level %d: %v", b.Name, idx+1, err)
			}
			return nil
		}
	}
	return nil
}
