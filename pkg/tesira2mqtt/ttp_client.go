package tesira2mqtt

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"regexp"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type TTPClient struct {
	conn         io.ReadWriteCloser
	connErrCount int

	responses chan *TTPResponse
	errors    chan *TTPError

	commandMutex sync.Mutex

	Blocks map[string]Block

	mqttClient mqtt.Client
}

func NewTTPClient() *TTPClient {
	return &TTPClient{
		responses: make(chan *TTPResponse, 100),
		errors:    make(chan *TTPError, 100),

		commandMutex: sync.Mutex{},
		Blocks:       make(map[string]Block),
		mqttClient:   nil,
	}
}

func (c *TTPClient) connect() error {
	var err error
	c.conn, err = DialSSH(context.Background(), config.TesiraSSH)

	if err != nil {
		return err
	}

	return nil
}

func (c *TTPClient) run() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.MQTT.Broker)
	opts.SetClientID("tesira2mqtt")
	opts.SetKeepAlive(2 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetAutoReconnect(true)

	opts.SetUsername(config.MQTT.Username)
	opts.SetPassword(config.MQTT.Password)

	opts.SetConnectionNotificationHandler(c.onMqttConnectionNotification)

	c.mqttClient = mqtt.NewClient(opts)
	if token := c.mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", token.Error())
	}
	c.mqttClient.Subscribe(fmt.Sprintf("%v#", config.MQTT.Prefix), 0, c.onMqttMessage)
	for {
		err := c.connect()
		if err != nil {
			c.connErrCount++
			if c.connErrCount > 10 {
				log.Fatalf("Failed to connect to Tesira after %d attempts: %v", c.connErrCount, err)
			}
			log.Printf("Error connecting to Tesira: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("Connected to Tesira at %s", config.TesiraSSH.Addr)

		go c.discoverBlocks()
		c.connErrCount = 0
		err = c.runReadLoop()
		if err != nil {
			log.Printf("Error in TTP read loop: %v", err)
		}
	}
}

func (c *TTPClient) onMqttMessage(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := string(msg.Payload())
	log.Printf("Received MQTT message on topic %s: %s", topic, payload)
	for name, block := range c.Blocks {
		err := block.HandleMQTTMessage(c, topic, payload)
		if err != nil {
			log.Printf("Error handling MQTT message for block %v: %v", name, err)
		}
	}
}

func (c *TTPClient) onMqttConnectionNotification(client mqtt.Client, notification mqtt.ConnectionNotification) {
	switch n := notification.(type) {
	case mqtt.ConnectionNotificationConnected:
		log.Printf("Connected to MQTT broker")
		c.publishAllBlockStates()
	case mqtt.ConnectionNotificationLost:
		log.Printf("Disconnected from MQTT broker: %v", n.Reason)
	}
}

func (c *TTPClient) publishAllBlockStates() {
	for name, block := range c.Blocks {
		err := block.PublishStateToMQTT(c)
		if err != nil {
			log.Printf("Error publishing state for block %v: %v", name, err)
		}
	}
}

func (c *TTPClient) runReadLoop() error {
	scanner := bufio.NewScanner(c.conn)
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return err
		}
		line := scanner.Text()
		parsed, err := ScanTTPLine(line)
		if err != nil {
			log.Printf("Error parsing TTP line: %v", err)
			continue
		}
		if parsed == nil {
			continue
		}

		switch v := parsed.(type) {
		case *TTPResponse:
			c.responses <- v
		case *TTPError:

			c.errors <- v
		case *TTPSubscriptionData:
			log.Printf("Received TTP subscription data: %+v", v)
			for _, block := range c.Blocks {
				changed, err := block.HandleTTPSubscriptionData(c, v)
				if err != nil {
					log.Printf("Error handling TTP subscription data for block: %v", err)
					continue
				}
				if changed {
					err = block.PublishStateToMQTT(c)
					if err != nil {
						log.Printf("Error publishing state to MQTT for block: %v", err)
					}
				}
			}
		default:
			log.Printf("Received unknown TTP message type: %T", v)
		}
	}
	return nil
}

var BlockTypeRegex = regexp.MustCompile(`\'BLOCKTYPE\' is not supported by ([A-Za-z0-9_]+)\:\:(?:Text)?Attributes`)

func (c *TTPClient) discoverBlocks() {

	_, err := c.RunCommand("SESSION set verbose true")
	if err != nil {
		log.Printf("Error setting verbose mode: %v", err)
	}
	_, err = c.RunCommand("SESSION set detailedResponse false")
	if err != nil {
		log.Printf("Error setting verbose mode: %v", err)
	}

	blocks, err := c.RunCommand("SESSION get aliases")
	if err != nil {
		log.Printf("Error setting verbose mode: %v", err)
	}

	for _, blockName := range blocks.Value.(map[string]any)["list"].([]any) {
		blockNameStr := blockName.(string)

		blockType := ""
		_, err := c.RunCommand(fmt.Sprintf("%s get BLOCKTYPE", blockNameStr))
		if err != nil {
			if ttpErr, ok := err.(*TTPError); ok {
				matches := BlockTypeRegex.FindStringSubmatch(ttpErr.Message)
				if len(matches) == 2 {
					blockType = matches[1]

				} else {
					log.Printf("Error discovering block type for %s: %v", blockNameStr, err)
				}
			} else {

				log.Printf("Error discovering block %s: %v", blockNameStr, err)
			}
		}

		var b Block
		switch blockType {

		case "LevelControlInterface":
			b = &LevelControlBlock{Name: blockNameStr}
		case "RoomCombinerInterface":
			b = &RoomCombinerBlock{Name: blockNameStr}
		}
		if b != nil {
			c.Blocks[blockNameStr] = b
			err := b.RunTTPDiscovery(c, blockNameStr)
			if err != nil {
				log.Printf("Error running TTP discovery for block %s: %v", blockNameStr, err)
			} else {
				log.Printf("Discovered block %s: %#v", blockNameStr, b)
			}
		}
	}

	c.publishAllBlockStates()

}

func (c *TTPClient) RunCommand(cmd string) (*TTPResponse, error) {
	c.commandMutex.Lock()
	defer c.commandMutex.Unlock()
	if c.conn == nil {
		return nil, io.ErrClosedPipe
	}
	_, err := c.conn.Write([]byte(cmd + "\n"))
	if err != nil {
		return nil, err
	}
	select {
	case resp := <-c.responses:

		return resp, nil
	case ttpErr := <-c.errors:

		return nil, ttpErr
	case <-time.After(5 * time.Second):
		return nil, io.ErrNoProgress
	}
}
