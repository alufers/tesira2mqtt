package tesira2mqtt

const BlockSubscriptionInterval = 300 //ms

type Block interface {
	RunTTPDiscovery(c *TTPClient, blockName string) error

	// HandleTTPSubscriptionData processes incoming Tesira subscription data for the block.
	// Returns true if the data has changed the blocks state
	HandleTTPSubscriptionData(c *TTPClient, data *TTPSubscriptionData) (bool, error)

	PublishStateToMQTT(c *TTPClient) error

	HandleMQTTMessage(c *TTPClient, topic string, payload string) error
}
