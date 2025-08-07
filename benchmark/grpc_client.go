package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "rocksdb_svc/go/proto"
)

// GRPCClient implements KVOperations interface using gRPC
type GRPCClient struct {
	client pb.KVStoreClient
	config *BenchmarkConfig
}

// NewGRPCClient creates a new gRPC client connection
func NewGRPCClient(serverAddr string, config *BenchmarkConfig) (*GRPCClient, func(), error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %v", err)
	}

	client := pb.NewKVStoreClient(conn)
	grpcClient := &GRPCClient{
		client: client,
		config: config,
	}

	// Return cleanup function
	cleanup := func() {
		conn.Close()
	}

	return grpcClient, cleanup, nil
}

// Put implements KVOperations interface for gRPC
func (g *GRPCClient) Put(key, value string) *BenchmarkResult {
	ctx, cancel := context.WithTimeout(context.Background(), g.config.Timeout)
	defer cancel()

	start := time.Now()
	_, err := g.client.Put(ctx, &pb.PutRequest{
		Key:   key,
		Value: value,
	})
	latency := time.Since(start)

	return &BenchmarkResult{
		Operation: "write",
		Latency:   latency,
		Success:   err == nil,
		Error:     err,
	}
}

// Get implements KVOperations interface for gRPC
func (g *GRPCClient) Get(key string) *BenchmarkResult {
	ctx, cancel := context.WithTimeout(context.Background(), g.config.Timeout)
	defer cancel()

	start := time.Now()
	_, err := g.client.Get(ctx, &pb.GetRequest{Key: key})
	latency := time.Since(start)

	return &BenchmarkResult{
		Operation: "read",
		Latency:   latency,
		Success:   err == nil,
		Error:     err,
	}
}

// Ping implements KVOperations interface for gRPC
func (g *GRPCClient) Ping(workerID int) *BenchmarkResult {
	ctx, cancel := context.WithTimeout(context.Background(), g.config.Timeout)
	defer cancel()

	// Create ping request with current timestamp
	timestamp := time.Now().UnixMicro()
	message := fmt.Sprintf("ping-%d", workerID)

	start := time.Now()
	resp, err := g.client.Ping(ctx, &pb.PingRequest{
		Message:   message,
		Timestamp: timestamp,
	})
	latency := time.Since(start)

	success := err == nil
	if success && resp != nil {
		// Optionally validate the response
		if resp.Message != message || resp.Timestamp != timestamp {
			success = false
			err = fmt.Errorf("ping response validation failed")
		}
	}

	return &BenchmarkResult{
		Operation: "ping",
		Latency:   latency,
		Success:   success,
		Error:     err,
	}
}
