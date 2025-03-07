package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kslog"
)

type FranzProducer struct {
	client *kgo.Client
}

func newFranzProducer(
	brokers []string,
	defaultTopic string,
	recordMaxTries int,
	metadataMaxAge time.Duration,
) (*FranzProducer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(defaultTopic),
		kgo.ClientID("test-client"),
		kgo.MetadataMaxAge(metadataMaxAge),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(1049000, true, false, nil)),

		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.MaxProduceRequestsInflightPerBroker(10),
		kgo.ProduceRequestTimeout(4500 * time.Millisecond),
		kgo.ProducerBatchMaxBytes(1049000),
		kgo.MaxBufferedRecords(10000),
		kgo.ProducerLinger(0),
		kgo.DisableIdempotentWrite(),
	}

	if recordMaxTries > 0 {
		opts = append(opts, kgo.RecordRetries(recordMaxTries))
	}

	opts = append(opts, kgo.WithLogger(kslog.New(slog.With("name", "kgo"))))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	client.ForceMetadataRefresh()

	return &FranzProducer{
		client: client,
	}, nil
}

func (f *FranzProducer) ProduceBatch(entries [][]byte) kgo.ProduceResults {
	if len(entries) == 0 {
		return nil
	}

	records := make([]*kgo.Record, len(entries))
	for i, payload := range entries {
		records[i] = &kgo.Record{
			Key:   nil,
			Value: payload,
		}
	}

	results := f.client.ProduceSync(context.Background(), records...)

	return results
}

func (f *FranzProducer) Close() error {
	f.client.Close()
	return nil
}
