package main

import (
	"context"
	"log"
	"time"

	"instrumentation/collector/opensearch"
)

func main() {
	ctx := context.Background()

	collector, err := opensearch.NewShardCollector(
		ctx,
		"http://localhost:3000",
		"localhost:4317",
	)
	if err != nil {
		log.Fatalf("Failed to create collector: %v", err)
	}
	defer collector.Shutdown(ctx)

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := collector.CollectMetrics(ctx); err != nil {
				log.Printf("Failed to collect metrics: %v", err)
			}
		}
	}
}
