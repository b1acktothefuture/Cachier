package coordinator

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Node struct {
	Host string `yaml:"Host"`
	Port uint64 `yaml:"Port"`
	// Parameters for secure connections [certificate_path]
}

type Config struct {
	Nodes                map[string]Node `yaml:"Nodes"`
	NumberOfVirtualNodes int             `yaml:"NumberOfVirtualNodes"`
	Log                  struct {
		File string `yaml:"File"`
	} `yaml:"Log"`
}

// takes in a config file descriptor
// Initializes the initial config : yaml reader
func InitConfig(confFile string) (*Config, error) {
	var config Config

	file, err := os.Open(confFile)
	if err != nil {
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
