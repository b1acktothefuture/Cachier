package utils

/*
TODO:
Use value pointers and key pointers
In the entry struct use something efficient for comparision
Implement custom hash function instead of using the one built in
*/

import (
	"fmt"
	"hash/fnv"
	"sync"
)

const (
	RED   = true
	BLACK = false
)

type TreeNode struct {
	entry  Entry
	left   *TreeNode
	right  *TreeNode
	color  bool // true for red, false for black
	parent *TreeNode
}

type Entry struct {
	Key   string
	Value []byte
	mutex sync.RWMutex
}

type Bucket struct {
	root *TreeNode
}

// Left rotate a node
func (b *Bucket) leftRotate(x *TreeNode) {
	y := x.right
	x.right = y.left
	if y != nil && y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		b.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.left = x
	x.parent = y
}

// Right rotate a node
func (b *Bucket) rightRotate(x *TreeNode) {
	y := x.left
	x.left = y.right
	if y != nil && y.right != nil {
		y.right.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		b.root = y
	} else if x == x.parent.right {
		x.parent.right = y
	} else {
		x.parent.left = y
	}
	y.right = x
	x.parent = y
}

// Rebalance the tree after insertion
func (b *Bucket) rebalanceAfterInsert(node *TreeNode) {
	for node != b.root && node.parent != nil && node.parent.color == RED {
		if node.parent == node.parent.parent.left {
			uncle := node.parent.parent.right
			if uncle != nil && uncle.color == RED {
				node.parent.color = BLACK
				uncle.color = BLACK
				node.parent.parent.color = RED
				node = node.parent.parent
			} else {
				if node == node.parent.right {
					node = node.parent
					b.leftRotate(node)
				}
				node.parent.color = BLACK
				node.parent.parent.color = RED
				b.rightRotate(node.parent.parent)
			}
		} else {
			uncle := node.parent.parent.left
			if uncle != nil && uncle.color == RED {
				node.parent.color = BLACK
				uncle.color = BLACK
				node.parent.parent.color = RED
				node = node.parent.parent
			} else {
				if node == node.parent.left {
					node = node.parent
					b.rightRotate(node)
				}
				node.parent.color = BLACK
				node.parent.parent.color = RED
				b.leftRotate(node.parent.parent)
			}
		}
	}
	b.root.color = BLACK
}

// Rebalance the tree after deletion
func (b *Bucket) rebalanceAfterDelete(node *TreeNode) {
	for node != nil && node != b.root && node.color == BLACK {
		if node.parent == nil {
			break
		}

		if node == node.parent.left {
			sibling := node.parent.right
			if sibling != nil && sibling.color == RED {
				sibling.color = BLACK
				node.parent.color = RED
				b.leftRotate(node.parent)
				sibling = node.parent.right
			}
			if sibling == nil || (sibling.left == nil || sibling.left.color == BLACK) && (sibling.right == nil || sibling.right.color == BLACK) {
				if sibling != nil {
					sibling.color = RED
				}
				node = node.parent
			} else {
				if sibling.right == nil || sibling.right.color == BLACK {
					if sibling.left != nil {
						sibling.left.color = BLACK
					}
					sibling.color = RED
					b.rightRotate(sibling)
					sibling = node.parent.right
				}
				if sibling != nil {
					sibling.color = node.parent.color
					node.parent.color = BLACK
					if sibling.right != nil {
						sibling.right.color = BLACK
					}
					b.leftRotate(node.parent)
					node = b.root
				}
			}
		} else {
			sibling := node.parent.left
			if sibling != nil && sibling.color == RED {
				sibling.color = BLACK
				node.parent.color = RED
				b.rightRotate(node.parent)
				sibling = node.parent.left
			}
			if sibling == nil || (sibling.left == nil || sibling.left.color == BLACK) && (sibling.right == nil || sibling.right.color == BLACK) {
				if sibling != nil {
					sibling.color = RED
				}
				node = node.parent
			} else {
				if sibling.left == nil || sibling.left.color == BLACK {
					if sibling.right != nil {
						sibling.right.color = BLACK
					}
					sibling.color = RED
					b.leftRotate(sibling)
					sibling = node.parent.left
				}
				if sibling != nil {
					sibling.color = node.parent.color
					node.parent.color = BLACK
					if sibling.left != nil {
						sibling.left.color = BLACK
					}
					b.rightRotate(node.parent)
					node = b.root
				}
			}
		}
	}

	if node != nil {
		node.color = BLACK
	}
}

// Find the minimum node in a subtree
func (b *Bucket) minimum(node *TreeNode) *TreeNode {
	for node.left != nil {
		node = node.left
	}
	return node
}

// Transplant replaces one subtree as a child of its parent with another subtree
func (b *Bucket) transplant(u, v *TreeNode) {
	if u.parent == nil {
		b.root = v
	} else if u == u.parent.left {
		u.parent.left = v
	} else {
		u.parent.right = v
	}
	if v != nil {
		v.parent = u.parent
	}
}

// insert inserts a new entry into the Red-Black Tree and ensures balancing.
func (b *Bucket) insert(key string, value []byte) {
	newNode := &TreeNode{
		entry: Entry{Key: key, Value: value},
		color: RED, // New nodes are always red initially
	}

	// Perform the standard BST insert
	if b.root == nil {
		b.root = newNode
		newNode.color = BLACK // Root is always black
	} else {
		current := b.root
		var parent *TreeNode

		// BST insert
		for current != nil {
			parent = current
			if key < current.entry.Key {
				current = current.left
			} else {
				current = current.right
			}
		}

		// Insert as the left or right child of the parent node
		if key < parent.entry.Key {
			parent.left = newNode
		} else {
			parent.right = newNode
		}
		newNode.parent = parent
	}

	// Rebalance the tree after insertion
	b.rebalanceAfterInsert(newNode)
}

// Delete a key from the Red-Black Tree
func (b *Bucket) delete(key string) bool {
	// Find the node to delete
	node, found := b.search(key)
	if !found {
		return false // Key not found
	}

	var y *TreeNode
	if node.left == nil || node.right == nil {
		y = node
	} else {
		// Find the successor (minimum node in the right subtree)
		y = b.minimum(node.right)
	}

	var x *TreeNode
	if y.left != nil {
		x = y.left
	} else {
		x = y.right
	}

	if x != nil {
		x.parent = y.parent
	}

	if y.parent == nil {
		b.root = x // If y is the root node, we update the root
	} else if y == y.parent.left {
		y.parent.left = x
	} else {
		y.parent.right = x
	}

	// If the node being deleted was not the one we originally found,
	// replace it with the successor's entry
	if y != node {
		node.entry = y.entry
	}

	// If y was black, we need to rebalance the tree
	if y.color == BLACK {
		b.rebalanceAfterDelete(x)
	}

	return true
}

// Search for a key in the Red-Black Tree
func (b *Bucket) search(key string) (*TreeNode, bool) {
	current := b.root
	for current != nil {
		if key == current.entry.Key {
			return current, true
		} else if key < current.entry.Key {
			current = current.left
		} else {
			current = current.right
		}
	}
	return nil, false // Key not found
}

// Do we want a resize functionality?
type HashTable struct {
	buckets    []*Bucket
	bucketSize int
	mtx        sync.RWMutex
}

func hashKey(key string, bucketSize int) int {
	// Use FNV-1a hash algorithm
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key)) // Compute the hash
	return int(hash.Sum64() % uint64(bucketSize))
}

// NewHashTable initializes a new hash table with the given number of buckets
func NewHashTable(numBuckets int) *HashTable {
	if numBuckets <= 0 {
		numBuckets = 16 // Default size
	}
	buckets := make([]*Bucket, numBuckets)
	for i := 0; i < numBuckets; i++ {
		buckets[i] = &Bucket{}
	}
	return &HashTable{
		buckets:    buckets,
		bucketSize: numBuckets,
	}
}

// Returns true if a new entry was added, false if an existing entry was updated.
func (ht *HashTable) Put(key string, value []byte, RInfo *CheckpointInfo) bool {
	bucketIndex := hashKey(key, ht.bucketSize)

	ht.mtx.Lock()
	defer ht.mtx.Unlock()

	// WAL
	if nil != RInfo {
		RInfo.WC <- WALRecord{Operation: "PUT", Key: key, Value: value}
	}

	node, isFound := ht.buckets[bucketIndex].search(key)

	if isFound {
		node.entry.Value = value
		return false
	}

	ht.buckets[bucketIndex].insert(key, value)
	return true
}

func (ht *HashTable) Get(key string) ([]byte, bool) {
	bucketIndex := hashKey(key, ht.bucketSize)

	ht.mtx.RLock()
	defer ht.mtx.RUnlock()

	node, isFound := ht.buckets[bucketIndex].search(key)

	if !isFound {
		return nil, false
	}

	value := make([]byte, len(node.entry.Value))
	copy(value, node.entry.Value)

	return value, true
}

func (ht *HashTable) Update(key string, value []byte, RInfo *CheckpointInfo) bool {

	bucketIndex := hashKey(key, ht.bucketSize)

	// TODO: Can have a lock at node level to avoid a full lock on the table
	ht.mtx.Lock()
	defer ht.mtx.Unlock()

	if nil != RInfo {
		RInfo.WC <- WALRecord{Operation: "UPDATE", Key: key, Value: value}
	}

	node, isFound := ht.buckets[bucketIndex].search(key)

	if !isFound {
		return false
	}

	node.entry.Value = value

	return true
}

func (ht *HashTable) Delete(key string, RInfo *CheckpointInfo) bool {
	bucketIndex := hashKey(key, ht.bucketSize)

	ht.mtx.Lock()
	defer ht.mtx.Unlock()
	if nil != RInfo {
		RInfo.WC <- WALRecord{Operation: "DELETE", Key: key}
	}
	return ht.buckets[bucketIndex].delete(key)
}

func (ht *HashTable) Print() {
	for _, bucket := range ht.buckets {
		stack := []*TreeNode{}
		current := bucket.root

		for current != nil || len(stack) > 0 {
			// Go to the leftmost node
			for current != nil {
				stack = append(stack, current)
				current = current.left
			}

			// Pop from stack and process node
			current = stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			fmt.Printf("%v : %v\n", current.entry.Key, string(current.entry.Value))

			// Move to the right node
			current = current.right
		}
	}
}
