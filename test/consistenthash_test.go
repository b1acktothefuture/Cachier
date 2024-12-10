package test

import (
	"testing"

	"github.com/b1acktothefuture/dht-system/internal/utils"
)

func TestConsistentHashing(t *testing.T) {
	// Initialize the consistent hashing implementation
	hash := utils.NewConsistentHash(21)

	// Add some nodes
	hash.AddNode("NodeA")
	hash.AddNode("NodeB")
	hash.AddNode("NodeC")

	// Map some keys to nodes
	keys := []string{"Key1", "Key2", "Key3", "Key4", "Key5"}
	keyToNode := make(map[string]string)

	// Record initial node mapping
	for _, key := range keys {
		node, _ := hash.GetNode(key)
		keyToNode[key] = node
	}

	// Add a new node and verify key redistribution
	hash.AddNode("NodeD")
	redistributed := 0
	for _, key := range keys {
		newNode, _ := hash.GetNode(key)
		if newNode != keyToNode[key] {
			redistributed++
		}
	}

	// Ensure that only a subset of keys are redistributed
	if redistributed == 0 {
		t.Error("No keys were redistributed after adding a new node")
	} else if redistributed == len(keys) {
		t.Error("All keys were redistributed after adding a new node, which violates consistent hashing properties")
	}

	// Remove a node and verify key redistribution
	hash.RemoveNode("NodeB")
	redistributed = 0
	for _, key := range keys {
		newNode, _ := hash.GetNode(key)
		if newNode != keyToNode[key] {
			redistributed++
		}
	}

	// Ensure that keys previously mapped to the removed node are redistributed
	if redistributed == 0 {
		t.Error("No keys were redistributed after removing a node")
	}
}
