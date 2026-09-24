// Package kafka wraps franz-go: an idempotent producer for the outbox relay and a consumer group helper.
package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
)

// Producer publishes outbox messages with acks=all and idempotent writes.
type Producer struct {
	client *kgo.Client
}

// ProducerOption tweaks the producer client.
type ProducerOption func(*producerConfig)

type producerConfig struct{ autoCreateTopics bool }

// WithAutoCreateTopics toggles broker-side topic auto-creation (default on for local development).
func WithAutoCreateTopics(on bool) ProducerOption {
	return func(c *producerConfig) { c.autoCreateTopics = on }
}

func NewProducer(brokers []string, clientID string, opts ...ProducerOption) (*Producer, error) {
	pc := producerConfig{autoCreateTopics: true}
	for _, o := range opts {
		o(&pc)
	}
	kopts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		// Idempotent producer is on by default in franz-go when acks=all.
	}
	if pc.autoCreateTopics {
		kopts = append(kopts, kgo.AllowAutoTopicCreation())
	}
	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer: %w", err)
	}
	return &Producer{client: cl}, nil
}

var _ outbox.Publisher = (*Producer)(nil)

// Publish sends all messages and waits for every ack. Returns an error if any message failed.
func (p *Producer) Publish(ctx context.Context, msgs []outbox.Message) error {
	records := make([]*kgo.Record, 0, len(msgs))
	for _, m := range msgs {
		r := &kgo.Record{Topic: m.Topic, Key: m.Key, Value: m.Value}
		for k, v := range m.Headers {
			r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		records = append(records, r)
	}
	var errs []error
	for _, res := range p.client.ProduceSync(ctx, records...) {
		if res.Err != nil {
			errs = append(errs, res.Err)
		}
	}
	return errors.Join(errs...)
}

// Ping checks broker connectivity (for /readyz).
func (p *Producer) Ping(ctx context.Context) error { return p.client.Ping(ctx) }

func (p *Producer) Close(context.Context) error {
	p.client.Close()
	return nil
}
