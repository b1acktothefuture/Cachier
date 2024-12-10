package node

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/b1acktothefuture/dht-system/internal/utils"
)

const WALFlushTimeSeconds = 1
const CheckpointDurationMinutes = 1

func WriteToWAL(done <-chan struct{}, rInfo *utils.CheckpointInfo) {
	if nil == rInfo {
		return
	}

	ticker := time.NewTicker(time.Duration(WALFlushTimeSeconds) * time.Second)
	defer ticker.Stop()

	file, err := os.OpenFile(rInfo.WALFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("Error opening WAL file : %w", err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for {
		select {
		case record, ok := <-rInfo.WC:
			if !ok {
				return
			}
			data, err := json.Marshal(record)
			if err != nil {
				log.Print("Error in marshilling wal log")
				continue
			}
			writer.Write(append(data, '\n'))
		case <-ticker.C:
			writer.Flush()
		case <-rInfo.TC:
			// Clear all the content of the file
			err := file.Truncate(0) // Truncate the file to size 0
			if err != nil {
				log.Printf("Error truncating WAL file: %v", err)
				continue
			}

			_, err = file.Seek(0, 0) // Reset the file pointer to the beginning
			if err != nil {
				log.Printf("Error seeking WAL file: %v", err)
			}
			writer.Reset(file) // Reset the buffered writer to avoid stale references
		case <-done:
			writer.Flush()
			close(rInfo.WC)
			// Write all the remaining values
			return
		}
	}
}

func Checkpoint(done <-chan struct{}, ht *utils.HashTable, rInfo *utils.CheckpointInfo) {
	if nil == rInfo {
		return
	}

	ticker := time.NewTicker(time.Duration(CheckpointDurationMinutes) * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Write to a checkpoint file
			// Issue: What if there are still entries present in WAL channel
			rInfo.TC <- struct{}{}
			utils.TakeCheckpoint(ht, rInfo)
		case <-done:
			return
		}
	}
}
