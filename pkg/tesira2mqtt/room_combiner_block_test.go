package tesira2mqtt

import (
	"bufio"
	"net"
	"reflect"
	"sync"
	"testing"
)

func TestRoomCombinerBlockDiscoveryAndWallControl(t *testing.T) {
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

			switch command {
			case "Combiner get wallState 1":
				if _, err := serverConn.Write([]byte("+OK \"value\":true\n")); err != nil {
					return
				}
			case "Combiner get wallState 2":
				if _, err := serverConn.Write([]byte("+OK \"value\":false\n")); err != nil {
					return
				}
			case "Combiner get wallState 3":
				if _, err := serverConn.Write([]byte("-ERR Invalid wall\n")); err != nil {
					return
				}
			case "Combiner get group 1":
				if _, err := serverConn.Write([]byte("+OK \"value\":1\n")); err != nil {
					return
				}
			case "Combiner get sourceSelection 1":
				if _, err := serverConn.Write([]byte("+OK \"value\":2\n")); err != nil {
					return
				}
			case "Combiner get group 2":
				if _, err := serverConn.Write([]byte("+OK \"value\":1\n")); err != nil {
					return
				}
			case "Combiner get sourceSelection 2":
				if _, err := serverConn.Write([]byte("+OK \"value\":3\n")); err != nil {
					return
				}
			case "Combiner get group 3":
				if _, err := serverConn.Write([]byte("-ERR Invalid room\n")); err != nil {
					return
				}
			default:
				if _, err := serverConn.Write([]byte("+OK\n")); err != nil {
					return
				}
			}
		}
	}()

	block := &RoomCombinerBlock{}
	if err := block.RunTTPDiscovery(client, "Combiner"); err != nil {
		t.Fatalf("RunTTPDiscovery failed: %v", err)
	}
	if !reflect.DeepEqual(block.Walls, []bool{true, false}) {
		t.Fatalf("Walls = %v, want [true false]", block.Walls)
	}
	if !reflect.DeepEqual(block.Groups, []int64{1, 1}) {
		t.Fatalf("Groups = %v, want [1 1]", block.Groups)
	}
	if !reflect.DeepEqual(block.SourceSelections, []int64{2, 3}) {
		t.Fatalf("SourceSelections = %v, want [2 3]", block.SourceSelections)
	}

	previousConfig := config
	config = &Tesira2MQTTConfig{MQTT: MQTTConfig{Prefix: "tesira2mqtt/"}}
	t.Cleanup(func() { config = previousConfig })

	if err := block.HandleMQTTMessage(client, "tesira2mqtt/Combiner/wall/2/set", "true"); err != nil {
		t.Fatalf("HandleMQTTMessage failed: %v", err)
	}
	if err := block.HandleMQTTMessage(client, "tesira2mqtt/Combiner/wall/1/set", "invalid"); err == nil {
		t.Fatal("HandleMQTTMessage accepted an invalid wall state")
	}
	if err := block.HandleMQTTMessage(client, "tesira2mqtt/Combiner/room/2/group/set", "2"); err != nil {
		t.Fatalf("HandleMQTTMessage failed: %v", err)
	}
	if err := block.HandleMQTTMessage(client, "tesira2mqtt/Combiner/room/1/source/set", "4"); err != nil {
		t.Fatalf("HandleMQTTMessage failed: %v", err)
	}
	if err := block.HandleMQTTMessage(client, "tesira2mqtt/Combiner/room/1/source/set", "invalid"); err == nil {
		t.Fatal("HandleMQTTMessage accepted an invalid source selection")
	}

	changed, err := block.HandleTTPSubscriptionData(client, &TTPSubscriptionData{
		PublishToken: "T2M_Combiner_ROOM_2_GROUP",
		Data:         int64(2),
	})
	if err != nil {
		t.Fatalf("HandleTTPSubscriptionData failed: %v", err)
	}
	if !changed || !reflect.DeepEqual(block.Groups, []int64{1, 2}) {
		t.Fatalf("subscription result = changed:%v groups:%v, want changed:true groups:[1 2]", changed, block.Groups)
	}

	mu.Lock()
	defer mu.Unlock()
	wantCommands := []string{
		"Combiner get wallState 1",
		"Combiner get wallState 2",
		"Combiner get wallState 3",
		"Combiner get group 1",
		"Combiner get sourceSelection 1",
		"Combiner get group 2",
		"Combiner get sourceSelection 2",
		"Combiner get group 3",
		"Combiner subscribe wallState 1 T2M_Combiner_WALL_1 300",
		"Combiner subscribe wallState 2 T2M_Combiner_WALL_2 300",
		"Combiner subscribe group 1 T2M_Combiner_ROOM_1_GROUP 300",
		"Combiner subscribe sourceSelection 1 T2M_Combiner_ROOM_1_SOURCE 300",
		"Combiner subscribe group 2 T2M_Combiner_ROOM_2_GROUP 300",
		"Combiner subscribe sourceSelection 2 T2M_Combiner_ROOM_2_SOURCE 300",
		"Combiner set wallState 2 true",
		"Combiner set group 2 2",
		"Combiner set sourceSelection 1 4",
	}
	if !reflect.DeepEqual(commands, wantCommands) {
		t.Fatalf("commands = %v, want %v", commands, wantCommands)
	}
}
