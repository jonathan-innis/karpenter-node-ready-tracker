package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"
)

// NodeEvent represents a single node lifecycle event
type NodeEvent struct {
	EventType string
	NodeName  string
	Timestamp time.Time
}

// NodeTiming holds calculated timing data for a node
type NodeTiming struct {
	NodeName                string
	LaunchedToJoinedSeconds *float64
	ProxyToReadySeconds     *float64
	TotalSeconds            *float64
	EventTimestamps         map[string]time.Time
}

// parseTimestamp handles different timestamp formats
func parseTimestamp(timestampStr string) (time.Time, error) {
	// Handle UTC format like 2025-07-01T19:51:34Z
	if strings.HasSuffix(timestampStr, "Z") {
		return time.Parse(time.RFC3339, timestampStr)
	}
	// Handle timezone format like 2025-07-01T12:52:06-07:00
	return time.Parse(time.RFC3339, timestampStr)
}

// processEvents parses the input data and calculates timing differences
func processEvents(reader io.Reader) ([]NodeTiming, error) {
	csvReader := csv.NewReader(reader)

	// Read all records
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV: %w", err)
	}

	// Skip header if present
	startIdx := 0
	if len(records) > 0 && strings.Contains(records[0][0], "Event") {
		startIdx = 1
	}

	// Group events by node
	nodeEvents := make(map[string]map[string]time.Time)

	for i := startIdx; i < len(records); i++ {
		record := records[i]
		if len(record) != 3 {
			continue
		}

		eventType := strings.TrimSpace(record[0])
		nodeName := strings.TrimSpace(record[1])
		timestampStr := strings.TrimSpace(record[2])

		timestamp, err := parseTimestamp(timestampStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing timestamp '%s': %v\n", timestampStr, err)
			continue
		}

		if nodeEvents[nodeName] == nil {
			nodeEvents[nodeName] = make(map[string]time.Time)
		}
		nodeEvents[nodeName][eventType] = timestamp
	}

	// Calculate timing differences
	var results []NodeTiming

	for nodeName, events := range nodeEvents {
		timing := NodeTiming{
			NodeName:        nodeName,
			EventTimestamps: events,
		}

		// Calculate NodeLaunched to NodeJoined time
		if launchedTime, hasLaunched := events["NodeLaunched"]; hasLaunched {
			if joinedTime, hasJoined := events["NodeJoined"]; hasJoined {
				diff := joinedTime.Sub(launchedTime).Seconds()
				timing.LaunchedToJoinedSeconds = &diff
			}
		}

		// Calculate AWSNodeKubeProxyScheduled to NodeReady time
		if proxyTime, hasProxy := events["AWSNodeKubeProxyScheduled"]; hasProxy {
			if readyTime, hasReady := events["NodeReady"]; hasReady {
				diff := readyTime.Sub(proxyTime).Seconds()
				timing.ProxyToReadySeconds = &diff
			}
		}

		// Calculate total time
		var total float64
		hasTotal := false

		if timing.LaunchedToJoinedSeconds != nil {
			total += *timing.LaunchedToJoinedSeconds
			hasTotal = true
		}
		if timing.ProxyToReadySeconds != nil {
			total += *timing.ProxyToReadySeconds
			hasTotal = true
		}

		if hasTotal {
			timing.TotalSeconds = &total
		}

		results = append(results, timing)
	}

	// Sort results by node name for consistent output
	sort.Slice(results, func(i, j int) bool {
		return results[i].NodeName < results[j].NodeName
	})

	return results, nil
}

// writeCSV outputs the results in CSV format
func writeCSV(results []NodeTiming, writer io.Writer) error {
	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	// Collect all unique event types for timestamp columns
	eventTypes := make(map[string]bool)
	for _, result := range results {
		for eventType := range result.EventTimestamps {
			eventTypes[eventType] = true
		}
	}

	// Sort event types for consistent column order
	var sortedEventTypes []string
	for eventType := range eventTypes {
		sortedEventTypes = append(sortedEventTypes, eventType)
	}
	sort.Strings(sortedEventTypes)

	// Build headers
	headers := []string{
		"node",
		"launched_to_joined_seconds",
		"proxy_to_ready_seconds",
		"total_seconds",
	}

	// Add timestamp headers
	for _, eventType := range sortedEventTypes {
		headers = append(headers, eventType)
	}

	// Write header
	if err := csvWriter.Write(headers); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Write data rows
	for _, result := range results {
		row := make([]string, len(headers))

		row[0] = result.NodeName

		if result.LaunchedToJoinedSeconds != nil {
			row[1] = fmt.Sprintf("%.2f", *result.LaunchedToJoinedSeconds)
		}

		if result.ProxyToReadySeconds != nil {
			row[2] = fmt.Sprintf("%.2f", *result.ProxyToReadySeconds)
		}

		if result.TotalSeconds != nil {
			row[3] = fmt.Sprintf("%.2f", *result.TotalSeconds)
		}

		// Add timestamp values
		for i, eventType := range sortedEventTypes {
			if timestamp, exists := result.EventTimestamps[eventType]; exists {
				row[4+i] = timestamp.Format(time.RFC3339)
			}
		}

		if err := csvWriter.Write(row); err != nil {
			return fmt.Errorf("error writing row: %w", err)
		}
	}

	return nil
}

// printSummary displays summary statistics
func printSummary(results []NodeTiming) {
	fmt.Fprintf(os.Stderr, "Summary Statistics:\n")
	fmt.Fprintf(os.Stderr, "Total nodes processed: %d\n", len(results))

	// Collect timing data for statistics
	var launchedToJoinedTimes []float64
	var proxyToReadyTimes []float64
	var totalTimes []float64

	for _, result := range results {
		if result.LaunchedToJoinedSeconds != nil {
			launchedToJoinedTimes = append(launchedToJoinedTimes, *result.LaunchedToJoinedSeconds)
		}
		if result.ProxyToReadySeconds != nil {
			proxyToReadyTimes = append(proxyToReadyTimes, *result.ProxyToReadySeconds)
		}
		if result.TotalSeconds != nil {
			totalTimes = append(totalTimes, *result.TotalSeconds)
		}
	}

	// Print statistics
	if len(launchedToJoinedTimes) > 0 {
		avg := average(launchedToJoinedTimes)
		min := minimum(launchedToJoinedTimes)
		max := maximum(launchedToJoinedTimes)
		fmt.Fprintf(os.Stderr, "NodeLaunched to NodeJoined - Avg: %.2fs, Min: %.2fs, Max: %.2fs\n", avg, min, max)
	}

	if len(proxyToReadyTimes) > 0 {
		avg := average(proxyToReadyTimes)
		min := minimum(proxyToReadyTimes)
		max := maximum(proxyToReadyTimes)
		fmt.Fprintf(os.Stderr, "AWSNodeKubeProxyScheduled to NodeReady - Avg: %.2fs, Min: %.2fs, Max: %.2fs\n", avg, min, max)
	}

	if len(totalTimes) > 0 {
		avg := average(totalTimes)
		min := minimum(totalTimes)
		max := maximum(totalTimes)
		fmt.Fprintf(os.Stderr, "Total time - Avg: %.2fs, Min: %.2fs, Max: %.2fs\n", avg, min, max)
	}

	fmt.Fprintf(os.Stderr, "\n")
}

// Helper functions for statistics
func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func minimum(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

func maximum(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}

func main() {
	// Command line flags
	inputFile := flag.String("i", "", "Input file (default: stdin)")
	outputFile := flag.String("o", "", "Output CSV file (default: stdout)")
	showSummary := flag.Bool("summary", false, "Show summary statistics")
	flag.Parse()

	if lo.FromPtr(inputFile) == "" || lo.FromPtr(outputFile) == "" {
		log.Fatal("input file AND output file must be provided")
		os.Exit(1)
	}

	// Open input
	var reader io.Reader
	if *inputFile != "" {
		file, err := os.Open(*inputFile)
		if err != nil {
			log.Fatalf("Error opening input file: %v", err)
		}
		defer file.Close()
		reader = file
	} else {
		reader = os.Stdin
	}

	// Process events
	results, err := processEvents(reader)
	if err != nil {
		log.Fatalf("Error processing events: %v", err)
	}

	// Show summary if requested
	if *showSummary {
		printSummary(results)
	}

	// Open output
	var writer io.Writer
	if *outputFile != "" {
		file, err := os.Create(*outputFile)
		if err != nil {
			log.Fatalf("Error creating output file: %v", err)
		}
		defer file.Close()
		writer = file
		fmt.Fprintf(os.Stderr, "CSV output written to %s\n", *outputFile)
	} else {
		writer = os.Stdout
	}

	// Write CSV output
	if err := writeCSV(results, writer); err != nil {
		log.Fatalf("Error writing CSV: %v", err)
	}
}
