package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "rocksdb_svc/go/proto"
)

// BenchmarkConfig holds configuration for the benchmark
type BenchmarkConfig struct {
	ServerAddr      string
	Mode            string
	NumThreads      int
	TotalRequests   int64
	WritePercentage int
	PingPercentage  int
	KeySize         int
	ValueSize       int
	Timeout         time.Duration
}

// BenchmarkResult holds results for a single operation
type BenchmarkResult struct {
	Operation string
	Latency   time.Duration
	Success   bool
	Error     error
}

// Statistics holds aggregated performance metrics
type Statistics struct {
	Operation      string
	Count          int64
	Success        int64
	Failed         int64
	MinLatency     time.Duration
	MaxLatency     time.Duration
	AvgLatency     time.Duration
	P50Latency     time.Duration
	P90Latency     time.Duration
	P95Latency     time.Duration
	P99Latency     time.Duration
	Throughput     float64 // operations per second
}

// Worker represents a benchmark worker
type Worker struct {
	id       int
	client   pb.KVStoreClient
	config   *BenchmarkConfig
	results  chan *BenchmarkResult
	wg       *sync.WaitGroup
	counter  *int64
}

func main() {
	// Command line flags
	var (
		addr            = flag.String("addr", "localhost:50051", "server address")
		mode            = flag.String("mode", "mixed", "benchmark mode: mixed, ping")
		numThreads      = flag.Int("threads", 32, "number of concurrent threads")
		totalRequests   = flag.Int64("requests", 100000, "total number of requests")
		writePercentage = flag.Int("write-pct", 30, "percentage of write operations (0-100)")
		pingPercentage  = flag.Int("ping-pct", 10, "percentage of ping operations (0-100)")
		keySize         = flag.Int("key-size", 16, "size of keys in bytes")
		valueSize       = flag.Int("value-size", 100, "size of values in bytes")
		timeout         = flag.Duration("timeout", 30*time.Second, "timeout for individual operations")
	)
	flag.Parse()

	if *writePercentage < 0 || *writePercentage > 100 {
		log.Fatal("Write percentage must be between 0 and 100")
	}

	if *pingPercentage < 0 || *pingPercentage > 100 {
		log.Fatal("Ping percentage must be between 0 and 100")
	}

	if *mode != "mixed" && *mode != "ping" {
		log.Fatal("Mode must be either 'mixed' or 'ping'")
	}

	// Auto-adjust mode based on parameters
	if *mode == "mixed" {
		if *writePercentage == 100 && *pingPercentage == 10 {
			// User probably wants write-only, adjust ping percentage
			*pingPercentage = 0
		} else if *writePercentage + *pingPercentage > 100 {
			log.Fatal("Write percentage + Ping percentage cannot exceed 100")
		}
	}

	config := &BenchmarkConfig{
		ServerAddr:      *addr,
		Mode:            *mode,
		NumThreads:      *numThreads,
		TotalRequests:   *totalRequests,
		WritePercentage: *writePercentage,
		PingPercentage:  *pingPercentage,
		KeySize:         *keySize,
		ValueSize:       *valueSize,
		Timeout:         *timeout,
	}

	fmt.Printf("=== RocksDB Service Benchmark ===\n")
	fmt.Printf("Server: %s\n", config.ServerAddr)
	fmt.Printf("Mode: %s\n", config.Mode)
	fmt.Printf("Threads: %d\n", config.NumThreads)
	fmt.Printf("Total Requests: %d\n", config.TotalRequests)
	if config.Mode == "mixed" {
		fmt.Printf("Write Percentage: %d%%\n", config.WritePercentage)
		fmt.Printf("Ping Percentage: %d%%\n", config.PingPercentage)
		fmt.Printf("Read Percentage: %d%%\n", 100-config.WritePercentage-config.PingPercentage)
		fmt.Printf("Key Size: %d bytes\n", config.KeySize)
		fmt.Printf("Value Size: %d bytes\n", config.ValueSize)
	}
	fmt.Printf("Timeout: %v\n", config.Timeout)
	fmt.Println()

	// Run benchmark
	if err := runBenchmark(config); err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}
}

func runBenchmark(config *BenchmarkConfig) error {
	// Create results channel
	results := make(chan *BenchmarkResult, config.TotalRequests)
	
	// Counter for completed requests
	var completedRequests int64
	
	// Wait group for workers
	var wg sync.WaitGroup
	
	// Start workers
	fmt.Printf("Starting %d workers...\n", config.NumThreads)
	startTime := time.Now()
	
	for i := 0; i < config.NumThreads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			// Connect to server
			conn, err := grpc.Dial(config.ServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Worker %d failed to connect: %v", workerID, err)
				return
			}
			defer conn.Close()
			
			client := pb.NewKVStoreClient(conn)
			worker := &Worker{
				id:      workerID,
				client:  client,
				config:  config,
				results: results,
				wg:      &wg,
				counter: &completedRequests,
			}
			
			worker.run()
		}(i)
	}
	
	// Start result collector
	done := make(chan bool)
	go func() {
		wg.Wait()
		close(results)
		done <- true
	}()
	
	// Collect results
	var allResults []*BenchmarkResult
	go func() {
		for result := range results {
			allResults = append(allResults, result)
		}
	}()
	
	// Wait for completion
	<-done
	endTime := time.Now()
	totalDuration := endTime.Sub(startTime)
	
	fmt.Printf("Benchmark completed in %v\n\n", totalDuration)
	
	// Analyze results
	analyzeResults(allResults, totalDuration)
	
	return nil
}

func (w *Worker) run() {
	for {
		// Check if we've reached the total request limit
		current := atomic.AddInt64(w.counter, 1)
		if current > w.config.TotalRequests {
			break
		}
		
		var result *BenchmarkResult
		
		if w.config.Mode == "ping" {
			// Ping-only mode
			result = w.performPing()
		} else {
			// Mixed mode - determine operation type based on percentages
			remainder := current % 100
			
			if remainder < int64(w.config.WritePercentage) {
				result = w.performWrite()
			} else if remainder < int64(w.config.WritePercentage + w.config.PingPercentage) {
				result = w.performPing()
			} else {
				result = w.performRead()
			}
		}
		
		w.results <- result
		
		// Progress reporting
		if current%10000 == 0 {
			fmt.Printf("Completed %d requests...\n", current)
		}
	}
}

func (w *Worker) performWrite() *BenchmarkResult {
	key := generateRandomString(w.config.KeySize)
	value := generateRandomString(w.config.ValueSize)
	
	ctx, cancel := context.WithTimeout(context.Background(), w.config.Timeout)
	defer cancel()
	
	start := time.Now()
	_, err := w.client.Put(ctx, &pb.PutRequest{
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

func (w *Worker) performRead() *BenchmarkResult {
	// For reads, we'll generate a random key that might or might not exist
	key := generateRandomString(w.config.KeySize)
	
	ctx, cancel := context.WithTimeout(context.Background(), w.config.Timeout)
	defer cancel()
	
	start := time.Now()
	_, err := w.client.Get(ctx, &pb.GetRequest{Key: key})
	latency := time.Since(start)
	
	return &BenchmarkResult{
		Operation: "read",
		Latency:   latency,
		Success:   err == nil,
		Error:     err,
	}
}

func (w *Worker) performPing() *BenchmarkResult {
	ctx, cancel := context.WithTimeout(context.Background(), w.config.Timeout)
	defer cancel()
	
	// Create ping request with current timestamp
	timestamp := time.Now().UnixMicro()
	message := fmt.Sprintf("ping-%d", w.id)
	
	start := time.Now()
	resp, err := w.client.Ping(ctx, &pb.PingRequest{
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

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	rand.Read(b)
	for i := range b {
		b[i] = charset[b[i]%byte(len(charset))]
	}
	return string(b)
}

func analyzeResults(results []*BenchmarkResult, totalDuration time.Duration) {
	// Separate results by operation type
	readResults := make([]*BenchmarkResult, 0)
	writeResults := make([]*BenchmarkResult, 0)
	pingResults := make([]*BenchmarkResult, 0)
	
	for _, result := range results {
		switch result.Operation {
		case "read":
			readResults = append(readResults, result)
		case "write":
			writeResults = append(writeResults, result)
		case "ping":
			pingResults = append(pingResults, result)
		}
	}
	
	// Calculate statistics
	fmt.Println("=== BENCHMARK RESULTS ===")
	fmt.Printf("Total Duration: %v\n", totalDuration)
	fmt.Printf("Total Requests: %d\n", len(results))
	fmt.Println()
	
	if len(readResults) > 0 {
		readStats := calculateStatistics("READ", readResults, totalDuration)
		printStatistics(readStats)
	}
	
	if len(writeResults) > 0 {
		writeStats := calculateStatistics("WRITE", writeResults, totalDuration)
		printStatistics(writeStats)
	}
	
	if len(pingResults) > 0 {
		pingStats := calculateStatistics("PING", pingResults, totalDuration)
		printStatistics(pingStats)
	}
	
	// Overall statistics
	if len(results) > 0 {
		overallStats := calculateStatistics("OVERALL", results, totalDuration)
		printStatistics(overallStats)
	}
}

func calculateStatistics(operation string, results []*BenchmarkResult, totalDuration time.Duration) *Statistics {
	if len(results) == 0 {
		return &Statistics{Operation: operation}
	}
	
	stats := &Statistics{
		Operation: operation,
		Count:     int64(len(results)),
	}
	
	var latencies []time.Duration
	var totalLatency time.Duration
	
	for _, result := range results {
		if result.Success {
			stats.Success++
		} else {
			stats.Failed++
		}
		
		latencies = append(latencies, result.Latency)
		totalLatency += result.Latency
	}
	
	// Sort latencies for percentile calculations
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	
	// Calculate basic statistics
	stats.MinLatency = latencies[0]
	stats.MaxLatency = latencies[len(latencies)-1]
	stats.AvgLatency = totalLatency / time.Duration(len(latencies))
	
	// Calculate percentiles
	stats.P50Latency = percentile(latencies, 50)
	stats.P90Latency = percentile(latencies, 90)
	stats.P95Latency = percentile(latencies, 95)
	stats.P99Latency = percentile(latencies, 99)
	
	// Calculate throughput (operations per second)
	stats.Throughput = float64(stats.Count) / totalDuration.Seconds()
	
	return stats
}

func percentile(latencies []time.Duration, p int) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	
	index := int(math.Ceil(float64(p)/100*float64(len(latencies)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(latencies) {
		index = len(latencies) - 1
	}
	
	return latencies[index]
}

func printStatistics(stats *Statistics) {
	fmt.Printf("=== %s STATISTICS ===\n", stats.Operation)
	fmt.Printf("Total Operations: %d\n", stats.Count)
	fmt.Printf("Successful: %d (%.2f%%)\n", stats.Success, float64(stats.Success)/float64(stats.Count)*100)
	fmt.Printf("Failed: %d (%.2f%%)\n", stats.Failed, float64(stats.Failed)/float64(stats.Count)*100)
	fmt.Printf("Throughput: %.2f ops/sec\n", stats.Throughput)
	fmt.Println()
	
	fmt.Println("Latency Statistics:")
	fmt.Printf("  Min:     %v\n", stats.MinLatency)
	fmt.Printf("  Max:     %v\n", stats.MaxLatency)
	fmt.Printf("  Average: %v\n", stats.AvgLatency)
	fmt.Printf("  P50:     %v\n", stats.P50Latency)
	fmt.Printf("  P90:     %v\n", stats.P90Latency)
	fmt.Printf("  P95:     %v\n", stats.P95Latency)
	fmt.Printf("  P99:     %v\n", stats.P99Latency)
	fmt.Println()
}
