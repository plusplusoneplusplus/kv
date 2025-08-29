package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "rocksdb_svc/proto"
)

// Global debug flag
var debugMode bool

// debugLog prints debug messages if debug mode is enabled
func debugLog(format string, args ...interface{}) {
	if debugMode {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// logOperation logs operation details with timing
func logOperation(operation string, startTime time.Time) {
	if debugMode {
		duration := time.Since(startTime)
		log.Printf("[DEBUG] Operation '%s' completed in %v", operation, duration)
	}
}

// logError logs errors with context
func logError(context, message string) {
	if debugMode {
		log.Printf("[ERROR] %s: %s", context, message)
	}
}

// logConnection logs connection events
func logConnection(event, address string) {
	if debugMode {
		log.Printf("[DEBUG] Connection event: %s - Address: %s", event, address)
	}
}

func main() {
	// Command line flags
	var (
		addr      = flag.String("addr", "localhost:50051", "server address")
		operation = flag.String("op", "get", "operation: get, put, delete, list")
		key       = flag.String("key", "", "key for operation")
		value     = flag.String("value", "", "value for put operation")
		prefix    = flag.String("prefix", "", "prefix for list operation")
		limit     = flag.Int("limit", 10, "limit for list operation")
		debug     = flag.Bool("debug", false, "enable debug logging")
		verbose   = flag.Bool("verbose", false, "enable verbose debug logging (alias for -debug)")
	)
	flag.Parse()

	// Set debug mode
	debugMode = *debug || *verbose || os.Getenv("KV_CLIENT_DEBUG") == "1"
	
	if debugMode {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
		debugLog("Debug mode enabled")
		debugLog("Configuration: addr=%s, operation=%s", *addr, *operation)
	}

	// Connect to server
	logConnection("Attempting connection", *addr)
	startTime := time.Now()
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logError("Connection", err.Error())
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	logConnection("Connection established", *addr)
	logOperation("connect", startTime)

	client := pb.NewKVStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	debugLog("Client created, executing operation: %s", *operation)

	switch *operation {
	case "get":
		if *key == "" {
			log.Fatal("Key is required for get operation")
		}
		debugLog("Executing GET operation for key: %s", *key)
		opStartTime := time.Now()
		resp, err := client.Get(ctx, &pb.GetRequest{Key: *key})
		if err != nil {
			logError("GET operation", err.Error())
			log.Fatalf("Get failed: %v", err)
		}
		logOperation("GET", opStartTime)
		if resp.Found {
			debugLog("GET operation found value (length: %d)", len(resp.Value))
			fmt.Printf("Value: %s\n", resp.Value)
		} else {
			debugLog("GET operation: key not found")
			fmt.Println("Key not found")
		}

	case "put":
		if *key == "" || *value == "" {
			log.Fatal("Both key and value are required for put operation")
		}
		debugLog("Executing PUT operation for key: %s (value_len: %d)", *key, len(*value))
		opStartTime := time.Now()
		resp, err := client.Put(ctx, &pb.PutRequest{Key: *key, Value: *value})
		if err != nil {
			logError("PUT operation", err.Error())
			log.Fatalf("Put failed: %v", err)
		}
		logOperation("PUT", opStartTime)
		if resp.Success {
			debugLog("PUT operation successful")
			fmt.Println("Put successful")
		} else {
			logError("PUT response", resp.Error)
			fmt.Printf("Put failed: %s\n", resp.Error)
		}

	case "delete":
		if *key == "" {
			log.Fatal("Key is required for delete operation")
		}
		debugLog("Executing DELETE operation for key: %s", *key)
		opStartTime := time.Now()
		resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: *key})
		if err != nil {
			logError("DELETE operation", err.Error())
			log.Fatalf("Delete failed: %v", err)
		}
		logOperation("DELETE", opStartTime)
		if resp.Success {
			debugLog("DELETE operation successful")
			fmt.Println("Delete successful")
		} else {
			logError("DELETE response", resp.Error)
			fmt.Printf("Delete failed: %s\n", resp.Error)
		}

	case "list":
		debugLog("Executing LIST operation (prefix: %s, limit: %d)", *prefix, *limit)
		opStartTime := time.Now()
		resp, err := client.ListKeys(ctx, &pb.ListKeysRequest{
			Prefix: *prefix,
			Limit:  int32(*limit),
		})
		if err != nil {
			logError("LIST operation", err.Error())
			log.Fatalf("List failed: %v", err)
		}
		logOperation("LIST", opStartTime)
		debugLog("LIST operation found %d keys", len(resp.Keys))
		fmt.Printf("Found %d keys:\n", len(resp.Keys))
		for _, k := range resp.Keys {
			fmt.Printf("  %s\n", k)
		}
		if debugMode {
			debugLog("All keys listed successfully")
		}

	default:
		fmt.Println("Unknown operation. Use: get, put, delete, or list")
		flag.Usage()
	}
}
