package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/tecbot/gorocksdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "rocksdb_svc/kvstore"
)

// Server implements the KVStore service
type Server struct {
	pb.UnimplementedKVStoreServer
	db *gorocksdb.DB
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
}

// NewServer creates a new KVStore server with RocksDB
func NewServer(dbPath string) (*Server, error) {
	// Set up RocksDB options
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	
	// Open database
	db, err := gorocksdb.OpenDb(opts, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// Create read and write options
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()

	return &Server{
		db: db,
		ro: ro,
		wo: wo,
	}, nil
}

// Close closes the database and cleans up resources
func (s *Server) Close() {
	if s.db != nil {
		s.db.Close()
	}
	if s.ro != nil {
		s.ro.Destroy()
	}
	if s.wo != nil {
		s.wo.Destroy()
	}
}

// Get retrieves a value by key
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	value, err := s.db.Get(s.ro, []byte(req.Key))
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

// Put stores a key-value pair
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.Key == "" {
		return &pb.PutResponse{
			Success: false,
			Error:   "key cannot be empty",
		}, nil
	}

	err := s.db.Put(s.wo, []byte(req.Key), []byte(req.Value))
	if err != nil {
		return &pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to put value: %v", err),
		}, nil
	}

	return &pb.PutResponse{
		Success: true,
		Error:   "",
	}, nil
}

// Delete removes a key-value pair
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.Key == "" {
		return &pb.DeleteResponse{
			Success: false,
			Error:   "key cannot be empty",
		}, nil
	}

	err := s.db.Delete(s.wo, []byte(req.Key))
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to delete key: %v", err),
		}, nil
	}

	return &pb.DeleteResponse{
		Success: true,
		Error:   "",
	}, nil
}

// ListKeys returns all keys with optional prefix filtering
func (s *Server) ListKeys(ctx context.Context, req *pb.ListKeysRequest) (*pb.ListKeysResponse, error) {
	it := s.db.NewIterator(s.ro)
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

func main() {
	// Create data directory if it doesn't exist
	dbPath := "./data/rocksdb"
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Create server
	server, err := NewServer(dbPath)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	defer server.Close()

	// Create gRPC server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, server)

	// Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		log.Println("Shutting down gRPC server...")
		grpcServer.GracefulStop()
	}()

	log.Println("Starting gRPC server on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
