package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "rocksdb_svc/go/proto"
)

func main() {
	// Command line flags
	var (
		addr    = flag.String("addr", "localhost:50051", "server address")
		count   = flag.Int("count", 10, "number of ping requests to send")
		timeout = flag.Duration("timeout", 5*time.Second, "timeout for each ping")
	)
	flag.Parse()

	// Connect to server
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVStoreClient(conn)

	fmt.Printf("PING %s\n", *addr)
	fmt.Println()

	var totalLatency time.Duration
	successCount := 0

	for i := 0; i < *count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		
		// Create ping request
		timestamp := time.Now().UnixMicro()
		message := fmt.Sprintf("ping-request-%d", i+1)

		start := time.Now()
		resp, err := client.Ping(ctx, &pb.PingRequest{
			Message:   message,
			Timestamp: timestamp,
		})
		latency := time.Since(start)
		cancel()

		if err != nil {
			fmt.Printf("Request %d: ERROR - %v\n", i+1, err)
		} else {
			successCount++
			totalLatency += latency
			
			// Calculate round-trip time and server processing time
			currentTime := time.Now().UnixMicro()
			roundTripTime := time.Duration(currentTime-timestamp) * time.Microsecond
			serverTime := time.Duration(resp.ServerTimestamp-timestamp) * time.Microsecond
			
			fmt.Printf("Request %d: seq=%d message='%s' time=%v server_time=%v rtt=%v\n", 
				i+1, i+1, resp.Message, latency, serverTime, roundTripTime)
		}

		// Wait a bit between requests
		if i < *count-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Println()
	fmt.Printf("--- ping statistics ---\n")
	fmt.Printf("%d packets transmitted, %d received, %.1f%% packet loss\n", 
		*count, successCount, float64(*count-successCount)/float64(*count)*100)
	
	if successCount > 0 {
		avgLatency := totalLatency / time.Duration(successCount)
		fmt.Printf("avg latency: %v\n", avgLatency)
	}
}
