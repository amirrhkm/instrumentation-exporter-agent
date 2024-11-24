package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

type ShardCollector struct {
	client        *http.Client
	endpoint      string
	meterProvider *sdkmetric.MeterProvider
	meter         metric.Meter
}

type ShardInfo struct {
	Index  string `json:"index"`
	Shard  string `json:"shard"`
	Prirep string `json:"prirep"`
	State  string `json:"state"`
	Docs   string `json:"docs"`
	Store  string `json:"store"`
	IP     string `json:"ip"`
	Node   string `json:"node"`
}

func NewShardCollector(ctx context.Context, endpoint string, collectorEndpoint string) (*ShardCollector, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("opensearch-shard-collector"),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(collectorEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(
				exporter,
				sdkmetric.WithInterval(10*time.Second),
			),
		),
	)
	otel.SetMeterProvider(meterProvider)

	meter := meterProvider.Meter("opensearch.shards")

	return &ShardCollector{
		client:        &http.Client{Timeout: 10 * time.Second},
		endpoint:      endpoint,
		meterProvider: meterProvider,
		meter:         meter,
	}, nil
}

func (c *ShardCollector) CollectMetrics(ctx context.Context) error {
	shardStoreSize, err := c.meter.Float64ObservableGauge(
		"opensearch.shard.store.size",
		metric.WithDescription("Size of the shard store in bytes"),
		metric.WithUnit("bytes"),
	)
	if err != nil {
		return fmt.Errorf("failed to create store size gauge: %w", err)
	}

	_, err = c.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		shards, err := c.fetchShardInfo(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch shard info: %w", err)
		}

		for _, shard := range shards {
			sizeInBytes, err := convertStoreToBytes(shard.Store)
			if err != nil {
				return fmt.Errorf("failed to convert store size: %w", err)
			}

			attrs := []attribute.KeyValue{
				attribute.String("index", shard.Index),
				attribute.String("shard", shard.Shard),
				attribute.String("prirep", shard.Prirep),
				attribute.String("state", shard.State),
				attribute.String("node", shard.Node),
				attribute.String("ip", shard.IP),
			}

			o.ObserveFloat64(shardStoreSize, sizeInBytes, metric.WithAttributes(attrs...))
		}
		return nil
	}, shardStoreSize)

	return err
}

func (c *ShardCollector) fetchShardInfo(ctx context.Context) ([]ShardInfo, error) {
	var allShards []ShardInfo

	indices := []string{"otlp-metrics", "otlp-logs"}

	for _, index := range indices {
		url := fmt.Sprintf("%s/_cat/shards/%s?format=json", c.endpoint, index)

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := c.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to execute request: %w", err)
		}
		defer resp.Body.Close()

		var shards []ShardInfo
		if err := json.NewDecoder(resp.Body).Decode(&shards); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		allShards = append(allShards, shards...)
	}

	return allShards, nil
}

func convertStoreToBytes(store string) (float64, error) {
	store = strings.TrimSpace(store)
	if store == "" {
		return 0, nil
	}

	var multiplier float64
	switch {
	case strings.HasSuffix(store, "kb"):
		multiplier = 1024
		store = strings.TrimSuffix(store, "kb")
	case strings.HasSuffix(store, "mb"):
		multiplier = 1024 * 1024
		store = strings.TrimSuffix(store, "mb")
	case strings.HasSuffix(store, "gb"):
		multiplier = 1024 * 1024 * 1024
		store = strings.TrimSuffix(store, "gb")
	default:
		return 0, fmt.Errorf("unknown size unit in: %s", store)
	}

	value, err := strconv.ParseFloat(store, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse size value: %w", err)
	}

	return value * multiplier, nil
}

func (c *ShardCollector) Shutdown(ctx context.Context) error {
	return c.meterProvider.Shutdown(ctx)
}
