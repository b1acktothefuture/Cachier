package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/b1acktothefuture/dht-system/gen"
	"github.com/b1acktothefuture/dht-system/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StorageServer struct {
	pb.UnimplementedStorageServer
	HashTable *utils.HashTable
	RInfo     *utils.CheckpointInfo
}

func NewStorageServer(config *Config) *StorageServer {
	storageServer := &StorageServer{}
	storageServer.HashTable = utils.NewHashTable(config.HashTable.NumBuckets)

	if config.Checkpoint.Enabled {
		storageServer.RInfo = &utils.CheckpointInfo{
			WC:             make(chan utils.WALRecord),
			WALFile:        config.Checkpoint.WALFile,
			TC:             make(chan struct{}),
			CheckPointFile: config.Checkpoint.CheckpointFile,
		}
	}
	return storageServer
}

func (s *StorageServer) Get(ctx context.Context, request *pb.StorageGetRequest) (*pb.StorageGetResponse, error) {
	if nil == request {
		log.Println("Empty request received")
		return nil, fmt.Errorf("Empty request")
	}

	log.Printf("Received Get request: key[%s]", request.Key)

	value, isFound := s.HashTable.Get(request.Key)
	if false == isFound {
		return &pb.StorageGetResponse{
			Found: false,
			Value: nil,
		}, nil
	}

	return &pb.StorageGetResponse{
		Found: true,
		Value: value,
	}, nil
}

func (s *StorageServer) Put(ctx context.Context, request *pb.StoragePutRequest) (*pb.StoragePutResponse, error) {
	if nil == request.Value {
		return nil, status.Errorf(codes.InvalidArgument, "Value cannot be empty")
	}

	log.Printf("Received Put request: Key[%s]/Value[%v]", request.Key, request.Value)

	isPresent := s.HashTable.Put(request.Key, request.Value, s.RInfo)

	return &pb.StoragePutResponse{
		IsUpdated: isPresent,
	}, nil
}

func (s *StorageServer) Update(ctx context.Context, request *pb.StorageUpdateRequest) (*pb.StorageUpdateResponse, error) {
	if nil == request {
		log.Println("Empty request received")
		return nil, fmt.Errorf("Empty request")
	}

	log.Printf("Received Update request: Key[%s]/Value[%v]", request.Key, request.Value)

	isPresent := s.HashTable.Update(request.Key, request.Value, s.RInfo)

	return &pb.StorageUpdateResponse{
		IsKeyPresent: isPresent,
	}, nil
}

func (s *StorageServer) Delete(ctx context.Context, request *pb.StorageDeleteRequest) (*pb.StorageDeleteResponse, error) {
	if nil == request {
		log.Println("Empty request received")
		return nil, fmt.Errorf("Empty request")
	}

	log.Printf("Received Delete request: Key[%s]", request.Key)

	isPresent := s.HashTable.Delete(request.Key, s.RInfo)
	return &pb.StorageDeleteResponse{
		IsKeyPresent: isPresent,
	}, nil
}

func ServeStorage(wg *sync.WaitGroup, config *Config) {
	defer wg.Done()
	walDoneChan := make(chan struct{})
	checkPointDoneChan := make(chan struct{})

	if nil == config {
		log.Printf("Nil config receieved")
		return
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Network.Port))
	if err != nil {
		log.Printf("Error starting up the server on port[%d] : %w", config.Network.Port, err)
		return
	}

	grpcServer := grpc.NewServer()
	storageServer := NewStorageServer(config)

	// TODO: Restore from file
	utils.CheckpointRestore(storageServer.HashTable, config.Recover.CheckpointFile, config.Recover.WALFile)

	// New thread for WAL and checkpoint, non blocking
	go WriteToWAL(walDoneChan, storageServer.RInfo)
	go Checkpoint(checkPointDoneChan, storageServer.HashTable, storageServer.RInfo)

	pb.RegisterStorageServer(grpcServer, storageServer)

	if err := grpcServer.Serve(listener); err != nil {
		log.Printf("Failed to serve: %w", err)
		return
	}

	if nil != storageServer.RInfo {
		walDoneChan <- struct{}{}
		checkPointDoneChan <- struct{}{}
	}

}
