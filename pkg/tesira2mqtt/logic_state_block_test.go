package tesira2mqtt

import (
	"bufio"
	"net"
	"reflect"
	"sync"
	"testing"
)

func TestLogicStateBlockDiscoveryAndControl(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	client := NewTTPClient()
	client.conn = clientConn
	go func() {
		_ = client.runReadLoop()
	}()

	var (
		commands []string
		mu       sync.Mutex
	)
	go func() {
		scanner := bufio.NewScanner(serverConn)
		for scanner.Scan() {
			command := scanner.Text()
			mu.Lock()
			commands = append(commands, command)
			mu.Unlock()

			var response string
			switch command {
			case "Logic get state 1":
				response = "+OK \"value\":true\n"
			case "Logic get state 2":
				response = "+OK \"value\":false\n"
			case "Logic get state 3":
				response = "-ERR Invalid channel\n"
			default:
				response = "+OK\n"
			}
			if _, err := serverConn.Write([]byte(response)); err != nil {
				return
			}
		}
	}()

	block := &LogicStateBlock{}
	if err := block.RunTTPDiscovery(client, "Logic"); err != nil {
		t.Fatalf("RunTTPDiscovery failed: %v", err)
	}
	if !reflect.DeepEqual(block.States, []bool{true, false}) {
		t.Fatalf("States = %v, want [true false]", block.States)
	}

	previousConfig := config
	config = &Tesira2MQTTConfig{MQTT: MQTTConfig{Prefix: "tesira2mqtt/"}}
	t.Cleanup(func() { config = previousConfig })

	if err := block.HandleMQTTMessage(client, "tesira2mqtt/Logic/state/2/set", "true"); err != nil {
		t.Fatalf("HandleMQTTMessage failed: %v", err)
	}
	if err := block.HandleMQTTMessage(client, "tesira2mqtt/Logic/state/1/set", "invalid"); err == nil {
		t.Fatal("HandleMQTTMessage accepted an invalid logic state")
	}

	changed, err := block.HandleTTPSubscriptionData(client, &TTPSubscriptionData{
		PublishToken: "T2M_Logic_STATE_2",
		Data:         true,
	})
	if err != nil {
		t.Fatalf("HandleTTPSubscriptionData failed: %v", err)
	}
	if !changed || !reflect.DeepEqual(block.States, []bool{true, true}) {
		t.Fatalf("subscription result = changed:%v states:%v, want changed:true states:[true true]", changed, block.States)
	}

	mu.Lock()
	defer mu.Unlock()
	wantCommands := []string{
		"Logic get state 1",
		"Logic get state 2",
		"Logic get state 3",
		"Logic subscribe state 1 T2M_Logic_STATE_1 300",
		"Logic subscribe state 2 T2M_Logic_STATE_2 300",
		"Logic set state 2 true",
	}
	if !reflect.DeepEqual(commands, wantCommands) {
		t.Fatalf("commands = %v, want %v", commands, wantCommands)
	}
}
