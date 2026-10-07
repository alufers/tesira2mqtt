package tesira2mqtt

import (
	"errors"
	"fmt"
	"strconv"
)

type LogicStateBlock struct {
	Name   string
	States []bool
}

func (b *LogicStateBlock) RunTTPDiscovery(c *TTPClient, blockName string) error {
	b.Name = blockName

	for channel := 1; ; channel++ {
		resp, err := c.RunCommand(fmt.Sprintf("%v get state %d", b.Name, channel))
		if err != nil {
			if channel == 1 {
				return fmt.Errorf("failed to get state for block %v: %v", blockName, err)
			}
			var ttpErr *TTPError
			if errors.As(err, &ttpErr) {
				break
			}
			return fmt.Errorf("failed to get state for block %v, channel %d: %v", blockName, channel, err)
		}

		state, ok := responseBoolValue(resp)
		if !ok {
			return fmt.Errorf("failed to convert state to bool for block %v, channel %d", blockName, channel)
		}
		b.States = append(b.States, state)
	}

	for channel := range b.States {
		_, err := c.RunCommand(fmt.Sprintf("%v subscribe state %d T2M_%v_STATE_%d %v", b.Name, channel+1, b.Name, channel+1, BlockSubscriptionInterval))
		if err != nil {
			return fmt.Errorf("failed to subscribe to state for block %v, channel %d: %v", blockName, channel+1, err)
		}
	}

	return nil
}

func (b *LogicStateBlock) HandleTTPSubscriptionData(_ *TTPClient, data *TTPSubscriptionData) (bool, error) {
	for channel := range b.States {
		if data.PublishToken != fmt.Sprintf("T2M_%v_STATE_%d", b.Name, channel+1) {
			continue
		}

		state, ok := data.Data.(bool)
		if !ok {
			return false, fmt.Errorf("failed to convert state to bool for block %v, channel %d", b.Name, channel+1)
		}
		b.States[channel] = state
		return true, nil
	}
	return false, nil
}

func (b *LogicStateBlock) PublishStateToMQTT(c *TTPClient) error {
	c.mqttClient.Publish(fmt.Sprintf("%v%v/type", config.MQTT.Prefix, b.Name), 0, true, "LogicState")
	c.mqttClient.Publish(fmt.Sprintf("%v%v/num_channels", config.MQTT.Prefix, b.Name), 0, true, fmt.Sprintf("%v", len(b.States)))
	for channel, state := range b.States {
		c.mqttClient.Publish(fmt.Sprintf("%v%v/state/%d", config.MQTT.Prefix, b.Name, channel+1), 0, true, fmt.Sprintf("%v", state))
	}
	return nil
}

func (b *LogicStateBlock) HandleMQTTMessage(c *TTPClient, topic string, payload string) error {
	for channel := range b.States {
		if topic != fmt.Sprintf("%v%v/state/%d/set", config.MQTT.Prefix, b.Name, channel+1) {
			continue
		}

		state, err := strconv.ParseBool(payload)
		if err != nil {
			return fmt.Errorf("failed to parse logic state from payload: %v", err)
		}
		_, err = c.RunCommand(fmt.Sprintf("%v set state %d %v", b.Name, channel+1, state))
		if err != nil {
			return fmt.Errorf("failed to set state for block %v, channel %d: %v", b.Name, channel+1, err)
		}
		return nil
	}
	return nil
}
