package utils

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
)

// ConsistentHash represents a consistent hashing ring with virtual nodes.
type ConsistentHash struct {
	VirtualNodes   int               // Number of virtual nodes for each physical node.
	hashSortedKeys []uint32          // Sorted keys for efficient lookup.
	hashRing       map[uint32]string // Mapping of hash keys to node names.
	nodes          map[string]bool   // Set of physical nodes.
}

// NewConsistentHash creates a new consistent hash instance with the specified number of virtual nodes.
func NewConsistentHash(virtualNodes int) *ConsistentHash {
	return &ConsistentHash{
		VirtualNodes:   virtualNodes,
		hashRing:       make(map[uint32]string),
		nodes:          make(map[string]bool),
		hashSortedKeys: make([]uint32, 0),
	}
}

// GetNode returns the closest node for the given object in the consistent hash ring.
func (ch *ConsistentHash) GetNode(obj string) (string, error) {
	if len(ch.nodes) == 0 {
		return "", errors.New("consistent hash ring is empty")
	}

	key := ch.hashKey(obj)
	index := ch.searchNearestKeyIndex(key)
	return ch.hashRing[ch.hashSortedKeys[index]], nil
}

// AddNode adds a node and its virtual nodes to the consistent hash ring.
func (ch *ConsistentHash) AddNode(node string) {
	if ch.nodes[node] {
		return // Node already exists
	}

	ch.nodes[node] = true

	// Add physical and virtual nodes
	for i := 0; i <= ch.VirtualNodes; i++ {
		virtualKey := ch.virtualNodeKey(i, node)
		ch.hashRing[virtualKey] = node
	}

	ch.updateSortedKeys()
}

// RemoveNode removes a node and its virtual nodes from the consistent hash ring.
func (ch *ConsistentHash) RemoveNode(node string) {
	if !ch.nodes[node] {
		return // Node does not exist
	}

	delete(ch.nodes, node)

	// Remove physical and virtual nodes
	for i := 0; i <= ch.VirtualNodes; i++ {
		virtualKey := ch.virtualNodeKey(i, node)
		delete(ch.hashRing, virtualKey)
	}

	ch.updateSortedKeys()
}

// ListNodes returns a list of all physical nodes in the consistent hash ring.
func (ch *ConsistentHash) ListNodes() []string {
	nodes := make([]string, 0, len(ch.nodes))
	for node := range ch.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// virtualNodeKey computes the hash key for a virtual node.
func (ch *ConsistentHash) virtualNodeKey(index int, node string) uint32 {
	return ch.hashKey(strconv.Itoa(index) + "-" + node)
}

// searchNearestKeyIndex finds the index of the nearest hash key in the ring for the given key.
func (ch *ConsistentHash) searchNearestKeyIndex(key uint32) int {
	index := sort.Search(len(ch.hashSortedKeys), func(i int) bool {
		return ch.hashSortedKeys[i] >= key
	})
	if index == len(ch.hashSortedKeys) {
		index = 0 // Wrap around to the start of the ring
	}
	return index
}

// updateSortedKeys refreshes the sorted list of hash keys in the ring.
func (ch *ConsistentHash) updateSortedKeys() {
	keys := make([]uint32, 0, len(ch.hashRing))
	for key := range ch.hashRing {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	ch.hashSortedKeys = keys
}

// hashKey calculates the CRC32 hash for a given object.
func (ch *ConsistentHash) hashKey(obj string) uint32 {
	return crc32.ChecksumIEEE([]byte(obj))
}
