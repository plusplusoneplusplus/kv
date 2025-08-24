package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/linxGnu/grocksdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "rocksdb_svc/proto"
)

// Server implements the KVStore service
type Server struct {
	pb.UnimplementedKVStoreServer
	db         *grocksdb.DB
	txnDB      *grocksdb.TransactionDB
	ro         *grocksdb.ReadOptions
	wo         *grocksdb.WriteOptions
	txnOptions *grocksdb.TransactionOptions
	
	// Concurrency control for read transactions
	readSemaphore  chan struct{}
	writeSemaphore chan struct{}
	mu             sync.RWMutex
}

// NewServer creates a new KVStore server with RocksDB Transaction Database
func NewServer(dbPath string) (*Server, error) {
	// Set up RocksDB options
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	
	// Set up transaction database options
	txnDBOptions := grocksdb.NewDefaultTransactionDBOptions()
	
	// Open transaction database (this enables pessimistic locking)
	txnDB, err := grocksdb.OpenTransactionDb(opts, txnDBOptions, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open transaction database: %v", err)
	}

	// Create read and write options
	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	
	// Create transaction options for pessimistic locking
	txnOptions := grocksdb.NewDefaultTransactionOptions()

	// Configure concurrency limits
	maxReadConcurrency := 32   // Limit concurrent read transactions
	maxWriteConcurrency := 16  // Limit concurrent write transactions

	return &Server{
		db:             nil, // We'll use txnDB instead
		txnDB:          txnDB,
		ro:             ro,
		wo:             wo,
		txnOptions:     txnOptions,
		readSemaphore:  make(chan struct{}, maxReadConcurrency),
		writeSemaphore: make(chan struct{}, maxWriteConcurrency),
	}, nil
}

// Close closes the database and cleans up resources
func (s *Server) Close() {
	if s.txnDB != nil {
		s.txnDB.Close()
	}
	if s.ro != nil {
		s.ro.Destroy()
	}
	if s.wo != nil {
		s.wo.Destroy()
	}
	if s.txnOptions != nil {
		s.txnOptions.Destroy()
	}
}

// Get retrieves a value by key using a transaction for consistency
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	// Acquire read semaphore to limit concurrent read transactions
	select {
	case s.readSemaphore <- struct{}{}:
		defer func() { <-s.readSemaphore }()
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, "timeout waiting for read transaction slot")
	}

	// Create a read-only transaction
	txn := s.txnDB.TransactionBegin(s.wo, s.txnOptions, nil)
	defer txn.Destroy()

	value, err := txn.Get(s.ro, []byte(req.Key))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get value: %v", err)
	}
	defer value.Free()

	if !value.Exists() {
		return &pb.GetResponse{
			Value: "",
			Found: false,
		}, nil
	}

	return &pb.GetResponse{
		Value: string(value.Data()),
		Found: true,
	}, nil
}

// Put stores a key-value pair using pessimistic transaction
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.Key == "" {
		return &pb.PutResponse{
			Success: false,
			Error:   "key cannot be empty",
		}, nil
	}

	// Acquire write semaphore to limit concurrent write transactions
	select {
	case s.writeSemaphore <- struct{}{}:
		defer func() { <-s.writeSemaphore }()
	case <-ctx.Done():
		return &pb.PutResponse{
			Success: false,
			Error:   "timeout waiting for write transaction slot",
		}, nil
	}

	// Create a transaction for pessimistic locking
	txn := s.txnDB.TransactionBegin(s.wo, s.txnOptions, nil)
	defer txn.Destroy()

	// Put the key-value pair within the transaction
	err := txn.Put([]byte(req.Key), []byte(req.Value))
	if err != nil {
		txn.Rollback()
		return &pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to put value: %v", err),
		}, nil
	}

	// Commit the transaction
	err = txn.Commit()
	if err != nil {
		return &pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to commit transaction: %v", err),
		}, nil
	}

	return &pb.PutResponse{
		Success: true,
		Error:   "",
	}, nil
}

// Delete removes a key-value pair using pessimistic transaction
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.Key == "" {
		return &pb.DeleteResponse{
			Success: false,
			Error:   "key cannot be empty",
		}, nil
	}

	// Acquire write semaphore to limit concurrent write transactions
	select {
	case s.writeSemaphore <- struct{}{}:
		defer func() { <-s.writeSemaphore }()
	case <-ctx.Done():
		return &pb.DeleteResponse{
			Success: false,
			Error:   "timeout waiting for write transaction slot",
		}, nil
	}

	// Create a transaction for pessimistic locking
	txn := s.txnDB.TransactionBegin(s.wo, s.txnOptions, nil)
	defer txn.Destroy()

	// Delete the key within the transaction
	err := txn.Delete([]byte(req.Key))
	if err != nil {
		txn.Rollback()
		return &pb.DeleteResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to delete key: %v", err),
		}, nil
	}

	// Commit the transaction
	err = txn.Commit()
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to commit transaction: %v", err),
		}, nil
	}

	return &pb.DeleteResponse{
		Success: true,
		Error:   "",
	}, nil
}

// ListKeys returns all keys with optional prefix filtering using a transaction
func (s *Server) ListKeys(ctx context.Context, req *pb.ListKeysRequest) (*pb.ListKeysResponse, error) {
	// Acquire read semaphore to limit concurrent read transactions
	select {
	case s.readSemaphore <- struct{}{}:
		defer func() { <-s.readSemaphore }()
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, "timeout waiting for read transaction slot")
	}

	// Create a read-only transaction for consistent snapshot
	txn := s.txnDB.TransactionBegin(s.wo, s.txnOptions, nil)
	defer txn.Destroy()

	it := txn.NewIterator(s.ro)
	defer it.Close()

	var keys []string
	count := 0
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 1000 // Default limit
	}

	if req.Prefix != "" {
		// Seek to the prefix
		it.Seek([]byte(req.Prefix))
	} else {
		// Start from the beginning
		it.SeekToFirst()
	}

	for it.Valid() && count < limit {
		key := string(it.Key().Data())
		
		// If prefix is specified, check if key starts with prefix
		if req.Prefix != "" && !strings.HasPrefix(key, req.Prefix) {
			break
		}
		
		keys = append(keys, key)
		count++
		it.Next()
	}

	if err := it.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterator error: %v", err)
	}

	return &pb.ListKeysResponse{
		Keys: keys,
	}, nil
}
