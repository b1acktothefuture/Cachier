package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	pb "github.com/b1acktothefuture/dht-system/gen"
)

func put(coordinator *Coordinator, key, value string) {

	nodeID, _ := coordinator.ConsistentHash.GetNode(key)
	req := &pb.StoragePutRequest{
		Key:   key,
		Value: []byte(value),
	}
	res, _ := coordinator.Nodes[nodeID].client.Put(context.Background(), req)
	// Error handling
	fmt.Printf("Node[%v] Is Updated : %v\n", nodeID, res.GetIsUpdated())
}

func get(coordinator *Coordinator, key string) {

	nodeID, _ := coordinator.ConsistentHash.GetNode(key)
	req := &pb.StorageGetRequest{
		Key: key,
	}

	res, _ := coordinator.Nodes[nodeID].client.Get(context.Background(), req)

	fmt.Printf("Node[%v] Value : %v\n", nodeID, string(res.GetValue()))
}

func update(coordinator *Coordinator, key, value string) {
	nodeID, _ := coordinator.ConsistentHash.GetNode(key)
	req := &pb.StorageUpdateRequest{
		Key:   key,
		Value: []byte(value),
	}

	res, _ := coordinator.Nodes[nodeID].client.Update(context.Background(), req)

	fmt.Printf("Node[%v] Update Status : %v\n", nodeID, res.IsKeyPresent)

}

func delete(coordinator *Coordinator, key string) {
	nodeID, _ := coordinator.ConsistentHash.GetNode(key)
	req := &pb.StorageDeleteRequest{
		Key: key,
	}

	res, _ := coordinator.Nodes[nodeID].client.Delete(context.Background(), req)

	fmt.Printf("Node[%v] Delete Status : %v\n", nodeID, res.IsKeyPresent)
}

func readInput() string {
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}

func cli(coordinator *Coordinator) {
	for {
		fmt.Print(">> ")
		input := readInput()
		parts := strings.Fields(input) // Split input into parts

		if len(parts) == 0 {
			fmt.Println("Invalid input. Please enter a valid command.")
			continue
		}

		command := strings.ToUpper(parts[0])

		switch command {
		case "GET":
			if len(parts) != 2 {
				fmt.Println("Invalid GET command. Usage: GET Key")
				continue
			}
			key := parts[1]
			get(coordinator, key)
		case "PUT":
			if len(parts) != 3 {
				fmt.Println("Invalid PUT command. Usage: PUT Key Value")
				continue
			}
			key, value := parts[1], parts[2]
			put(coordinator, key, value)
		case "DELETE":
			if len(parts) != 2 {
				fmt.Println("Invalid DELETE command. Usage: DELETE Key")
				continue
			}
			key := parts[1]
			delete(coordinator, key)
		case "UPDATE":
			if len(parts) != 3 {
				fmt.Println("Invalid UPDATE command. Usage: UPDATE Key")
				continue
			}
			key, value := parts[1], parts[2]
			update(coordinator, key, value)
		case "EXIT":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid command. Please enter a valid command.")
		}
	}
}
