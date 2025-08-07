package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// BenchmarkConfig holds configuration for the benchmark
type BenchmarkConfig struct {
	ServerAddr        string
	Mode              string
	NumThreads        int
	TotalRequests     int64
	WritePercentage   int
	PingPercentage    int
	KeySize           int
	ValueSize         int
	Timeout           time.Duration
	Protocol          string // "grpc" or "thrift"
	PrePopulateKeys   int    // Number of keys to pre-populate for reads
	PrePopulatedKeys  []string // Slice to store pre-populated keys for reads
}

// KVOperations defines the interface for key-value operations
type KVOperations interface {
	Put(key, value string) *BenchmarkResult
	Get(key string) *BenchmarkResult
	Ping(workerID int) *BenchmarkResult
}

// ClientFactory defines the interface for creating client connections
type ClientFactory interface {
	CreateClient(serverAddr string, config *BenchmarkConfig) (KVOperations, func(), error)
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
	id        int
	kvOps     KVOperations
	config    *BenchmarkConfig
	results   chan *BenchmarkResult
	wg        *sync.WaitGroup
	counter   *int64
}

// GRPCClientFactory implements ClientFactory for gRPC
type GRPCClientFactory struct{}

func (f *GRPCClientFactory) CreateClient(serverAddr string, config *BenchmarkConfig) (KVOperations, func(), error) {
	return NewGRPCClient(serverAddr, config)
}

func main() {
	// Command line flags
	var (
		addr             = flag.String("addr", "", "server address (defaults: grpc=localhost:50051, thrift=localhost:9090)")
		mode             = flag.String("mode", "mixed", "benchmark mode: ping, read, write, mixed")
		numThreads       = flag.Int("threads", 32, "number of concurrent threads")
		totalRequests    = flag.Int64("requests", 100000, "total number of requests")
		writePercentage  = flag.Int("write-pct", 30, "percentage of write operations (0-100) - only used in mixed mode")
		pingPercentage   = flag.Int("ping-pct", 0, "percentage of ping operations (0-100) - only used in mixed mode")
		keySize          = flag.Int("key-size", 16, "size of keys in bytes")
		valueSize        = flag.Int("value-size", 100, "size of values in bytes")
		timeout          = flag.Duration("timeout", 30*time.Second, "timeout for individual operations")
		protocol         = flag.String("protocol", "grpc", "protocol to use: grpc, thrift, raw")
		prePopulateKeys  = flag.Int("prepopulate", 10000, "number of keys to pre-populate for read operations")
	)
	flag.Parse()

	// Set default address based on protocol if not specified
	if *addr == "" {
		switch *protocol {
		case "grpc":
			*addr = "localhost:50051"
		case "thrift":
			*addr = "localhost:9090"
		case "raw":
			*addr = "./data/rocksdb-benchmark" // local database path for raw mode
		default:
			*addr = "localhost:50051" // fallback to gRPC default
		}
	}

	// Show help with examples if requested
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		showUsageExamples()
		return
	}

	// Validate mode
	validModes := map[string]bool{
		"ping":  true,
		"read":  true,
		"write": true,
		"mixed": true,
	}
	if !validModes[*mode] {
		log.Fatal("Mode must be one of: ping, read, write, mixed")
	}

	// Validate protocol
	validProtocols := map[string]bool{
		"grpc":   true,
		"thrift": true,
		"raw":    true,
	}
	if !validProtocols[*protocol] {
		log.Fatal("Protocol must be one of: grpc, thrift, raw")
	}

	// Validate percentages only for mixed mode
	if *mode == "mixed" {
		if *writePercentage < 0 || *writePercentage > 100 {
			log.Fatal("Write percentage must be between 0 and 100")
		}

		if *pingPercentage < 0 || *pingPercentage > 100 {
			log.Fatal("Ping percentage must be between 0 and 100")
		}

		if *writePercentage + *pingPercentage > 100 {
			log.Fatal("Write percentage + Ping percentage cannot exceed 100")
		}
	}

	config := &BenchmarkConfig{
		ServerAddr:       *addr,
		Mode:             *mode,
		NumThreads:       *numThreads,
		TotalRequests:    *totalRequests,
		WritePercentage:  *writePercentage,
		PingPercentage:   *pingPercentage,
		KeySize:          *keySize,
		ValueSize:        *valueSize,
		Timeout:          *timeout,
		Protocol:         *protocol,
		PrePopulateKeys:  *prePopulateKeys,
		PrePopulatedKeys: make([]string, 0, *prePopulateKeys),
	}

	fmt.Printf("=== RocksDB Service Benchmark ===\n")
	fmt.Printf("Server: %s\n", config.ServerAddr)
	fmt.Printf("Protocol: %s\n", config.Protocol)
	fmt.Printf("Mode: %s\n", config.Mode)
	fmt.Printf("Threads: %d\n", config.NumThreads)
	fmt.Printf("Total Requests: %d\n", config.TotalRequests)
	
	if config.Mode == "mixed" {
		fmt.Printf("Write Percentage: %d%%\n", config.WritePercentage)
		fmt.Printf("Ping Percentage: %d%%\n", config.PingPercentage)
		fmt.Printf("Read Percentage: %d%%\n", 100-config.WritePercentage-config.PingPercentage)
	}
	
	if config.Mode == "read" || config.Mode == "write" || config.Mode == "mixed" {
		fmt.Printf("Key Size: %d bytes\n", config.KeySize)
		fmt.Printf("Value Size: %d bytes\n", config.ValueSize)
		if config.Mode == "read" || (config.Mode == "mixed" && 100-config.WritePercentage-config.PingPercentage > 0) {
			fmt.Printf("Pre-populate Keys: %d\n", config.PrePopulateKeys)
		}
	}
	
	fmt.Printf("Timeout: %v\n", config.Timeout)
	fmt.Println()

	// Run benchmark
	if err := runBenchmark(config); err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}
}

func runBenchmark(config *BenchmarkConfig) error {
	// Select client factory based on protocol
	var clientFactory ClientFactory
	switch config.Protocol {
	case "grpc":
		clientFactory = &GRPCClientFactory{}
	case "thrift":
		clientFactory = &ThriftClientFactory{}
	case "raw":
		clientFactory = &RawClientFactory{}
	default:
		return fmt.Errorf("unsupported protocol: %s", config.Protocol)
	}

	// Pre-populate database with keys for read operations if needed
	needsPrePopulation := config.Mode == "read" || 
		(config.Mode == "mixed" && 100-config.WritePercentage-config.PingPercentage > 0)
	
	if needsPrePopulation && config.PrePopulateKeys > 0 {
		fmt.Printf("Pre-populating database with %d keys...\n", config.PrePopulateKeys)
		if err := prePopulateDatabase(clientFactory, config); err != nil {
			return fmt.Errorf("failed to pre-populate database: %v", err)
		}
		fmt.Printf("Pre-population completed.\n\n")
	}

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
			
			// Create client connection
			kvClient, cleanup, err := clientFactory.CreateClient(config.ServerAddr, config)
			if err != nil {
				log.Printf("Worker %d failed to connect: %v", workerID, err)
				return
			}
			defer cleanup()
			
			worker := &Worker{
				id:      workerID,
				kvOps:   kvClient,
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

// prePopulateDatabase fills the database with keys for read operations
func prePopulateDatabase(clientFactory ClientFactory, config *BenchmarkConfig) error {
	// Create a client for pre-population
	kvClient, cleanup, err := clientFactory.CreateClient(config.ServerAddr, config)
	if err != nil {
		return fmt.Errorf("failed to create client for pre-population: %v", err)
	}
	defer cleanup()

	// Generate and store keys
	for i := 0; i < config.PrePopulateKeys; i++ {
		// Create a unique key with consistent format
		keyPrefix := fmt.Sprintf("benchmark_key_%08d", i)
		remainingKeySize := config.KeySize - len(keyPrefix)
		
		var key string
		if remainingKeySize > 0 {
			// Add random suffix if there's remaining space
			randomSuffix := generateRandomString(remainingKeySize)
			key = keyPrefix + randomSuffix
		} else {
			// Use just the prefix if key size is too small
			key = keyPrefix[:config.KeySize]
		}
		
		value := generateRandomString(config.ValueSize)
		
		result := kvClient.Put(key, value)
		if !result.Success {
			return fmt.Errorf("failed to pre-populate key %s: %v", key, result.Error)
		}
		
		// Store the key for later use in reads
		config.PrePopulatedKeys = append(config.PrePopulatedKeys, key)
		
		// Progress reporting for large pre-populations
		if (i+1)%1000 == 0 {
			fmt.Printf("Pre-populated %d/%d keys...\n", i+1, config.PrePopulateKeys)
		}
	}
	
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
		
		switch w.config.Mode {
		case "ping":
			result = w.kvOps.Ping(w.id)
		case "read":
			key := w.getReadKey()
			result = w.kvOps.Get(key)
		case "write":
			key := generateRandomString(w.config.KeySize)
			value := generateRandomString(w.config.ValueSize)
			result = w.kvOps.Put(key, value)
		case "mixed":
			// Mixed mode - determine operation type based on percentages
			remainder := current % 100
			
			if remainder < int64(w.config.WritePercentage) {
				key := generateRandomString(w.config.KeySize)
				value := generateRandomString(w.config.ValueSize)
				result = w.kvOps.Put(key, value)
			} else if remainder < int64(w.config.WritePercentage + w.config.PingPercentage) {
				result = w.kvOps.Ping(w.id)
			} else {
				key := w.getReadKey()
				result = w.kvOps.Get(key)
			}
		}
		
		w.results <- result
		
		// Progress reporting
		if current%10000 == 0 {
			fmt.Printf("Completed %d requests...\n", current)
		}
	}
}

// getReadKey returns a key for read operations, either from pre-populated keys or generates a random one
func (w *Worker) getReadKey() string {
	if len(w.config.PrePopulatedKeys) > 0 {
		// Use a pre-populated key (with some randomness to distribute load)
		index := int(time.Now().UnixNano()) % len(w.config.PrePopulatedKeys)
		return w.config.PrePopulatedKeys[index]
	}
	// Fallback to random key if no pre-populated keys available
	return generateRandomString(w.config.KeySize)
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
	
	// Operations summary table
	fmt.Printf("┌─────────────────────┬─────────────┬──────────────┬──────────────┐\n")
	fmt.Printf("│ %-19s │ %-11s │ %-12s │ %-12s │\n", "Metric", "Total", "Successful", "Failed")
	fmt.Printf("├─────────────────────┼─────────────┼──────────────┼──────────────┤\n")
	fmt.Printf("│ %-19s │ %-11d │ %-12d │ %-12d │\n", "Operations", stats.Count, stats.Success, stats.Failed)
	fmt.Printf("│ %-19s │ %-11s │ %11.2f%% │ %11.2f%% │\n", "Percentage", "-", 
		float64(stats.Success)/float64(stats.Count)*100, float64(stats.Failed)/float64(stats.Count)*100)
	fmt.Printf("│ %-19s │ %-11.2f │ %-12s │ %-12s │\n", "Throughput (op/s)", stats.Throughput, "-", "-")
	fmt.Printf("└─────────────────────┴─────────────┴──────────────┴──────────────┘\n")
	fmt.Println()
	
	// Latency statistics table
	fmt.Printf("┌─────────────────────┬─────────────────┐\n")
	fmt.Printf("│ %-19s │ %-15s │\n", "Latency Metric", "Value")
	fmt.Printf("├─────────────────────┼─────────────────┤\n")
	fmt.Printf("│ %-19s │ %-15v │\n", "Minimum", stats.MinLatency)
	fmt.Printf("│ %-19s │ %-15v │\n", "Average", stats.AvgLatency)
	fmt.Printf("│ %-19s │ %-15v │\n", "50th Percentile", stats.P50Latency)
	fmt.Printf("│ %-19s │ %-15v │\n", "90th Percentile", stats.P90Latency)
	fmt.Printf("│ %-19s │ %-15v │\n", "95th Percentile", stats.P95Latency)
	fmt.Printf("│ %-19s │ %-15v │\n", "99th Percentile", stats.P99Latency)
	fmt.Printf("│ %-19s │ %-15v │\n", "Maximum", stats.MaxLatency)
	fmt.Printf("└─────────────────────┴─────────────────┘\n")
	fmt.Println()
}

func showUsageExamples() {
	fmt.Println("RocksDB Service Benchmark Tool")
	fmt.Println()
	fmt.Println("Usage Examples:")
	fmt.Println()
	fmt.Println("1. Ping-only benchmark with gRPC:")
	fmt.Println("   ./benchmark -protocol=grpc -mode=ping -requests=10000 -threads=16")
	fmt.Println()
	fmt.Println("2. Read-only benchmark with Thrift:")
	fmt.Println("   ./benchmark -protocol=thrift -mode=read -requests=50000 -threads=32 -key-size=20")
	fmt.Println()
	fmt.Println("3. Write-only benchmark (default gRPC):")
	fmt.Println("   ./benchmark -mode=write -requests=25000 -threads=16 -value-size=1024")
	fmt.Println()
	fmt.Println("4. Mixed workload (default):")
	fmt.Println("   ./benchmark -mode=mixed -write-pct=30 -requests=100000")
	fmt.Println("   # This creates: 30% writes, 70% reads (ping-pct defaults to 0)")
	fmt.Println()
	fmt.Println("5. Mixed workload with pings:")
	fmt.Println("   ./benchmark -mode=mixed -write-pct=30 -ping-pct=10 -requests=100000")
	fmt.Println("   # This creates: 30% writes, 10% pings, 60% reads")
	fmt.Println()
	fmt.Println("6. Custom server address with protocol:")
	fmt.Println("   ./benchmark -protocol=grpc -addr=192.168.1.100:50051 -mode=ping")
	fmt.Println("   ./benchmark -protocol=thrift -addr=192.168.1.100:9090 -mode=ping")
	fmt.Println()
	fmt.Println("7. Using default addresses:")
	fmt.Println("   ./benchmark -protocol=grpc -mode=ping    # uses localhost:50051")
	fmt.Println("   ./benchmark -protocol=thrift -mode=ping  # uses localhost:9090")
	fmt.Println("   ./benchmark -protocol=raw -mode=ping     # uses ./data/rocksdb-benchmark")
	fmt.Println()
	fmt.Println("8. Raw RocksDB benchmarking (no network overhead):")
	fmt.Println("   ./benchmark -protocol=raw -mode=write -requests=100000 -threads=16")
	fmt.Println("   ./benchmark -protocol=raw -mode=mixed -write-pct=30 -requests=50000")
	fmt.Println("   ./benchmark -protocol=raw -addr=/tmp/my-benchmark-db -mode=read")
	fmt.Println()
	fmt.Println("Available Flags:")
	flag.PrintDefaults()
}
