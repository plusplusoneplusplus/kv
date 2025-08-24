package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "rocksdb_svc/proto"
)

func main() {
	// Command line flags
	var (
		addr      = flag.String("addr", "localhost:50051", "server address")
		operation = flag.String("op", "get", "operation: get, put, delete, list")
		key       = flag.String("key", "", "key for operation")
		value     = flag.String("value", "", "value for put operation")
		prefix    = flag.String("prefix", "", "prefix for list operation")
		limit     = flag.Int("limit", 10, "limit for list operation")
	)
	flag.Parse()

	// Connect to server
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch *operation {
	case "get":
		if *key == "" {
			log.Fatal("Key is required for get operation")
		}
		resp, err := client.Get(ctx, &pb.GetRequest{Key: *key})
		if err != nil {
			log.Fatalf("Get failed: %v", err)
		}
		if resp.Found {
			fmt.Printf("Value: %s\n", resp.Value)
		} else {
			fmt.Println("Key not found")
		}

	case "put":
		if *key == "" || *value == "" {
			log.Fatal("Both key and value are required for put operation")
		}
		resp, err := client.Put(ctx, &pb.PutRequest{Key: *key, Value: *value})
		if err != nil {
			log.Fatalf("Put failed: %v", err)
		}
		if resp.Success {
			fmt.Println("Put successful")
		} else {
			fmt.Printf("Put failed: %s\n", resp.Error)
		}

	case "delete":
		if *key == "" {
			log.Fatal("Key is required for delete operation")
		}
		resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: *key})
		if err != nil {
			log.Fatalf("Delete failed: %v", err)
		}
		if resp.Success {
			fmt.Println("Delete successful")
		} else {
			fmt.Printf("Delete failed: %s\n", resp.Error)
		}

	case "list":
		resp, err := client.ListKeys(ctx, &pb.ListKeysRequest{
			Prefix: *prefix,
			Limit:  int32(*limit),
		})
		if err != nil {
			log.Fatalf("List failed: %v", err)
		}
		fmt.Printf("Found %d keys:\n", len(resp.Keys))
		for _, k := range resp.Keys {
			fmt.Printf("  %s\n", k)
		}

	default:
		fmt.Println("Unknown operation. Use: get, put, delete, or list")
		flag.Usage()
	}
}
