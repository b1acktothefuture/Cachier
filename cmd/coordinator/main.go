package main

import (
	"flag"
	"log"
	"os"
	"sync"

	"github.com/b1acktothefuture/dht-system/internal/coordinator"
)

/*
TODOS
Iterator
Secure connection
>> TLS
Connection Pool
>> As of now a new go routine is spawned for every request, can we have a pool of threads for the same
Maintain a cache here
>> Hash table + Bloom filter for this. Can use LFU cache
Health endpoints
>> This can check Mem util and CPU Util and alert the user for the sam
*/

func main() {
	var wg sync.WaitGroup

	configFilePath := flag.String("config", "", "Path to the configuration file")
	flag.Parse()

	config, err := coordinator.InitConfig(*configFilePath)
	if err != nil {
		log.Printf("Error reading/parsing config : %w", err)
		return
	}

	// Init log config
	logFile, err := os.OpenFile(config.Log.File, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error opening log file")
		return
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	wg.Add(1)
	go coordinator.StorageCoordinator(config, &wg)

	wg.Wait()
}
