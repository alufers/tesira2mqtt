package tesira2mqtt

import "time"

const BlockSubscriptionInterval = 300 //ms

// StateResyncInterval is how often all block states are re-read from the Tesira.
const StateResyncInterval = 5 * time.Minute

type Block interface {
	RunTTPDiscovery(c *TTPClient, blockName string) error

	// RefreshState re-reads the current state from the Tesira without re-subscribing.
	RefreshState(c *TTPClient) error

	// HandleTTPSubscriptionData processes incoming Tesira subscription data for the block.
	// Returns true if the data has changed the blocks state
	HandleTTPSubscriptionData(c *TTPClient, data *TTPSubscriptionData) (bool, error)

	PublishStateToMQTT(c *TTPClient) error

	HandleMQTTMessage(c *TTPClient, topic string, payload string) error
}
