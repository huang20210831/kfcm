package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

const defaultTimeout = 10 * time.Second

func fetchMetadata(broker string, topics ...string) (*kafka.MetadataResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	metadata, err := newClient(broker).Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
	if err != nil {
		return nil, fmt.Errorf("fetch metadata from broker %s: %w", broker, err)
	}
	return metadata, nil
}

func newClient(broker string) *kafka.Client {
	return &kafka.Client{
		Addr:    kafka.TCP(broker),
		Timeout: defaultTimeout,
	}
}
