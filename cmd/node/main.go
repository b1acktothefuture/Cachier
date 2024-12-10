package main

import (
	"flag"
	"log"
	"os"
	"sync"

	"github.com/b1acktothefuture/dht-system/internal/node"
)

/*
TODOs
Elasticity
Read operation could be exposed as an iterator
Each server may implement thread pool
*/

func main() {
	var wg sync.WaitGroup
	var config *node.Config

	configFilePath := flag.String("config", "", "Path to the configuration file")
	flag.Parse()

	// Parse the configuration
	config, err := node.InitConfig(*configFilePath)
	if err != nil {
		log.Println("Error in initializing config")
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
	go node.ServeStorage(&wg, config)

	wg.Wait()
}
