package utils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// WALRecord represents a single operation in the WAL
type WALRecord struct {
	Operation string `json:"operation"` // "PUT" or "DELETE"
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"` // Empty for "DELETE"
}

type CheckPointRecord struct {
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

type CheckpointInfo struct {
	WALFile        string
	CheckPointFile string
	WC             chan WALRecord // WAL Channel
	TC             chan struct{}
}

func CheckpointRestore(ht *HashTable, checkpointFile *string, walFile *string) error {
	if nil != checkpointFile {
		chkpt, err := os.OpenFile(*checkpointFile, os.O_RDONLY, 0644)
		if err != nil {
			return fmt.Errorf("Error opening Checkpoint file : %w", err)
		}
		defer chkpt.Close()

		chkpt.Seek(0, 0)
		scanner := bufio.NewScanner(chkpt)
		for scanner.Scan() {
			var record CheckPointRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				return err
			}
			ht.Put(record.Key, record.Value, nil)
		}
		if nil != scanner.Err() {
			return scanner.Err()
		}
	}

	if nil != walFile {
		wal, err := os.OpenFile(*walFile, os.O_RDONLY, 0644)
		if err != nil {
			return fmt.Errorf("Error opening WAL file : %w", err)
		}
		defer wal.Close()

		wal.Seek(0, 0)
		scanner := bufio.NewScanner(wal)

		for scanner.Scan() {
			var record WALRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				return err
			}

			switch record.Operation {
			case "PUT":
				// Validate
				ht.Put(record.Key, record.Value, nil)
			case "DELETE":
				// Validate
				ht.Delete(record.Key, nil)
			case "UPDATE":
				// Validate
				ht.Update(record.Key, record.Value, nil)
			}
		}

	}
	return nil
}

func RecoverFromWAL(ht *HashTable, walFile string) error {
	wal, err := os.OpenFile(walFile, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("Error opening WAL file : %w", err)
	}
	defer wal.Close()

	wal.Seek(0, 0)
	scanner := bufio.NewScanner(wal)

	for scanner.Scan() {
		var record WALRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return err
		}

		switch record.Operation {
		case "PUT":
			// Validate
			ht.Put(record.Key, record.Value, nil)
		case "DELETE":
			// Validate
			ht.Delete(record.Key, nil)
		case "UPDATE":
			// Validate
			ht.Update(record.Key, record.Value, nil)
		}
	}
	return scanner.Err()
}

func TakeCheckpoint(ht *HashTable, rInfo *CheckpointInfo) error {
	ht.mtx.RLock()
	defer ht.mtx.RUnlock()

	checkpointFile, err := os.Create(rInfo.CheckPointFile)
	if err != nil {
		return fmt.Errorf("Failed to create checkpoint file: %v", err)
	}
	defer checkpointFile.Close()

	writer := bufio.NewWriter(checkpointFile)
	// Iterate over the hash table
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

			data, err := json.Marshal(CheckPointRecord{Key: current.entry.Key, Value: current.entry.Value})
			if err != nil {
				return fmt.Errorf("Error in marshilling wal log: %v", err)
			}
			writer.Write(append(data, '\n'))
			writer.Flush()

			// Move to the right node
			current = current.right
		}
	}
	return nil
}
