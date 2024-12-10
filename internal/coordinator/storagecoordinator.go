package coordinator

import (
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/b1acktothefuture/dht-system/gen"
	"github.com/b1acktothefuture/dht-system/internal/utils"
	"google.golang.org/grpc"
)

const connectionTimeout = 5

type NodeConnection struct {
	conn   *grpc.ClientConn
	client pb.StorageClient
}

type Coordinator struct {
	Nodes          map[string]*NodeConnection
	ConsistentHash *utils.ConsistentHash
}

func StorageCoordinator(config *Config, wg *sync.WaitGroup) {
	defer wg.Done()
	if nil == config {
		log.Println("Nil config received")
		return
	}

	coordinator := &Coordinator{
		Nodes:          make(map[string]*NodeConnection),
		ConsistentHash: utils.NewConsistentHash(config.NumberOfVirtualNodes),
	}

	// Setup
	for nodeID, nodeNetInfo := range config.Nodes {
		conn, err := grpc.Dial(fmt.Sprintf("%s:%d", nodeNetInfo.Host, nodeNetInfo.Port),
			grpc.WithBlock(), grpc.WithTimeout(connectionTimeout*time.Second), grpc.WithInsecure())
		if nil != err {
			log.Printf("Error establishing connection with : %s", fmt.Sprintf("%s:%d", nodeNetInfo.Host, nodeNetInfo.Port))
			return
		}
		defer conn.Close()
		coordinator.Nodes[nodeID] = &NodeConnection{
			conn:   conn,
			client: pb.NewStorageClient(conn),
		}
		coordinator.ConsistentHash.AddNode(nodeID)
	}

	// Call CLI with this config
	cli(coordinator)
}
