package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"rocksdb_svc/go/thrift/kvstore"
)

// ThriftClient implements KVOperations interface using Thrift
type ThriftClient struct {
	client    kvstore.KVStore
	transport thrift.TTransport
	config    *BenchmarkConfig
}

// NewThriftClient creates a new Thrift client connection
func NewThriftClient(serverAddr string, config *BenchmarkConfig) (*ThriftClient, func(), error) {
	// Parse server address
	host, port, err := net.SplitHostPort(serverAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse server address: %v", err)
	}

	// Create socket transport
	transport := thrift.NewTSocketConf(net.JoinHostPort(host, port), &thrift.TConfiguration{
		ConnectTimeout: config.Timeout,
		SocketTimeout:  config.Timeout,
	})

	// Use buffered transport to match the Rust server configuration
	bufferedTransport := thrift.NewTBufferedTransport(transport, 8192)

	// Create binary protocol with strict mode enabled to match server
	protocolFactory := thrift.NewTBinaryProtocolFactoryConf(&thrift.TConfiguration{
		TBinaryStrictRead:  thrift.BoolPtr(true),
		TBinaryStrictWrite: thrift.BoolPtr(true),
	})
	protocol := protocolFactory.GetProtocol(bufferedTransport)

	// Create Thrift client
	client := kvstore.NewKVStoreClient(thrift.NewTStandardClient(protocol, protocol))

	// Open connection
	if err := bufferedTransport.Open(); err != nil {
		return nil, nil, fmt.Errorf("failed to open thrift connection: %v", err)
	}

	thriftClient := &ThriftClient{
		client:    client,
		transport: bufferedTransport,
		config:    config,
	}

	// Return cleanup function
	cleanup := func() {
		if thriftClient.transport != nil {
			thriftClient.transport.Close()
		}
	}

	return thriftClient, cleanup, nil
}

// ThriftClientFactory implements ClientFactory for Thrift
type ThriftClientFactory struct{}

func (f *ThriftClientFactory) CreateClient(serverAddr string, config *BenchmarkConfig) (KVOperations, func(), error) {
	return NewThriftClient(serverAddr, config)
}

// Put implements KVOperations interface for Thrift
func (t *ThriftClient) Put(key, value string) *BenchmarkResult {
	ctx, cancel := context.WithTimeout(context.Background(), t.config.Timeout)
	defer cancel()

	start := time.Now()
	
	request := &kvstore.PutRequest{
		Key:   key,
		Value: value,
	}
	
	resp, err := t.client.Put(ctx, request)
	latency := time.Since(start)

	success := err == nil && resp != nil && resp.Success
	if err == nil && resp != nil && !resp.Success && resp.Error != nil && *resp.Error != "" {
		err = fmt.Errorf("server error: %s", *resp.Error)
	}

	return &BenchmarkResult{
		Operation: "write",
		Latency:   latency,
		Success:   success,
		Error:     err,
	}
}

// Get implements KVOperations interface for Thrift
func (t *ThriftClient) Get(key string) *BenchmarkResult {
	ctx, cancel := context.WithTimeout(context.Background(), t.config.Timeout)
	defer cancel()

	start := time.Now()
	
	request := &kvstore.GetRequest{
		Key: key,
	}
	
	resp, err := t.client.Get(ctx, request)
	latency := time.Since(start)

	success := err == nil && resp != nil
	// Note: For Get operations, we consider it successful even if the key is not found
	// The 'Found' field in the response indicates whether the key was actually found

	return &BenchmarkResult{
		Operation: "read",
		Latency:   latency,
		Success:   success,
		Error:     err,
	}
}

// Ping implements KVOperations interface for Thrift
func (t *ThriftClient) Ping(workerID int) *BenchmarkResult {
	ctx, cancel := context.WithTimeout(context.Background(), t.config.Timeout)
	defer cancel()

	// Create ping request with current timestamp
	timestamp := time.Now().UnixMicro()
	message := fmt.Sprintf("ping-%d", workerID)

	start := time.Now()
	
	request := &kvstore.PingRequest{
		Message:   &message,
		Timestamp: &timestamp,
	}
	
	resp, err := t.client.Ping(ctx, request)
	latency := time.Since(start)

	success := err == nil && resp != nil
	if success {
		// Optionally validate the response
		if resp.Message != message || resp.Timestamp != timestamp {
			success = false
			err = fmt.Errorf("ping response validation failed: expected message=%s timestamp=%d, got message=%s timestamp=%d", 
				message, timestamp, resp.Message, resp.Timestamp)
		}
	}

	return &BenchmarkResult{
		Operation: "ping",
		Latency:   latency,
		Success:   success,
		Error:     err,
	}
}
