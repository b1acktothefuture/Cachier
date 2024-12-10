package test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/b1acktothefuture/dht-system/internal/utils"
)

// Test function for Get, Put, Update, and Delete
func TestHashTable(t *testing.T) {
	ht := utils.NewHashTable(10)

	// Test Put and Get
	key := "foo"
	value := []byte("bar")
	if success := ht.Put(key, value, nil); !success {
		t.Fatalf("Put failed for key: %s", key)
	}
	if got, ok := ht.Get(key); !ok || string(got) != string(value) {
		t.Errorf("Get failed for key: %s, expected: %s, got: %s", key, value, got)
	}

	// Test Update
	newValue := []byte("baz")
	if success := ht.Update(key, newValue, nil); !success {
		t.Fatalf("Update failed for key: %s", key)
	}
	if got, ok := ht.Get(key); !ok || string(got) != string(newValue) {
		t.Errorf("Get failed after update for key: %s, expected: %s, got: %s", key, newValue, got)
	}

	// Test Delete
	if success := ht.Delete(key, nil); !success {
		t.Fatalf("Delete failed for key: %s", key)
	}
	if _, ok := ht.Get(key); ok {
		t.Errorf("Key %s still exists after deletion", key)
	}

	// Test non-existent key
	if success := ht.Update("nonexistent", []byte("value"), nil); success {
		t.Errorf("Update should have failed for non-existent key")
	}
	if success := ht.Delete("nonexistent", nil); success {
		t.Errorf("Delete should have failed for non-existent key")
	}
}

// Test concurrent access for thread safety
func TestHashTableConcurrency(t *testing.T) {
	ht := utils.NewHashTable(10)
	const numThreads = 1000

	// Put values concurrently
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprint(i)
			value := []byte(fmt.Sprint(i))
			ht.Put(key, value, nil)
		}(i)
	}
	wg.Wait()

	// Get values concurrently and check
	var checkWG sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		checkWG.Add(1)
		go func(i int) {
			defer checkWG.Done()
			key := fmt.Sprint(i)
			expectedValue := []byte(fmt.Sprint(i))
			if got, ok := ht.Get(key); !ok || string(got) != string(expectedValue) {
				t.Errorf("Concurrent access failed for key: %s, expected: %s, got: %s", key, expectedValue, got)
			}
		}(i)
	}
	checkWG.Wait()
}

// Test for edge cases like empty key and empty value
func TestEdgeCases(t *testing.T) {
	ht := utils.NewHashTable(10)

	// Test empty key
	if success := ht.Put("", []byte("value"), nil); !success {
		t.Fatal("Put should not fail with an empty key")
	}
	if value, ok := ht.Get(""); !ok || string(value) != "value" {
		t.Fatalf("Failed to get value for empty key")
	}

	// Test empty value
	if success := ht.Put("key", []byte(""), nil); !success {
		t.Fatal("Put should not fail with an empty value")
	}
	if value, ok := ht.Get("key"); !ok || string(value) != "" {
		t.Fatalf("Failed to get empty value for key")
	}

	// Test Delete empty key
	if success := ht.Delete("", nil); !success {
		t.Fatal("Delete should not fail for empty key")
	}
	if _, ok := ht.Get(""); ok {
		t.Fatal("Empty key should be deleted")
	}

	// Test Update with empty value
	if success := ht.Update("key", []byte("new value"), nil); !success {
		t.Fatal("Update should succeed for key")
	}
	if value, ok := ht.Get("key"); !ok || string(value) != "new value" {
		t.Fatalf("Failed to update value for key")
	}
}
