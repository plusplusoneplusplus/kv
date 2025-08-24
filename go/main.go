package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	pb "rocksdb_svc/proto"
)

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
