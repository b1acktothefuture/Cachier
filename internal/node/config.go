package node

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// TODO: Convert to pointers
// Better to detect if config was present
// Config struct defines the configuration for the node
type Config struct {
	NodeID string `yaml:"NodeID"`

	Network struct {
		Port uint64 `yaml:"Port"`
	} `yaml:"Network"`

	HashTable struct {
		NumBuckets int `yaml:"NumBuckets"`
	} `yaml:"HashTable"`

	Log struct {
		File string `yaml:"File"`
	} `yaml:"Log"`

	Checkpoint struct {
		Enabled        bool   `yaml:"Enabled"`
		CheckpointFile string `yaml:"CheckpointFile"`
		WALFile        string `yaml:"WALFile"`
	} `yaml:"Checkpoint"`

	Recover struct {
		CheckpointFile *string `yaml:"CheckpointFile"`
		WALFile        *string `yaml:"WALFile"`
	} `yaml:"Recover"`
}

// takes in a config file descriptor
// Initializes the initial config : yaml reader
func InitConfig(confFile string) (*Config, error) {
	var config Config

	file, err := os.Open(confFile)
	if err != nil {
		// error handling
		return nil, err
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("Failed to decode config file: %v", err)
	}

	// TODO: Validate config

	return &config, nil
}
