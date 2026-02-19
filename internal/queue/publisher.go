package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const (
	streamMaxLen     int64 = 100000
	pipelineBatchMax       = 500
)

type Publisher struct {
	rdb *redis.Client
}

func NewPublisher(rdb *redis.Client) *Publisher {
	return &Publisher{rdb: rdb}
}

func (p *Publisher) PublishURL(ctx context.Context, msg URLMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling url message: %w", err)
	}
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: FrontierStream,
		MaxLen: streamMaxLen,
		Approx: true,
		Values: map[string]interface{}{"payload": body},
	}).Err()
}

func (p *Publisher) PublishParse(ctx context.Context, msg ParseMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling parse message: %w", err)
	}
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: ParseStream,
		MaxLen: streamMaxLen,
		Approx: true,
		Values: map[string]interface{}{"payload": body},
	}).Err()
}

// PublishURLBatch pipelines multiple URL messages into the frontier stream.
// Large batches are chunked to avoid excessive memory usage in Redis pipelines.
func (p *Publisher) PublishURLBatch(ctx context.Context, msgs []URLMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	for i := 0; i < len(msgs); i += pipelineBatchMax {
		end := i + pipelineBatchMax
		if end > len(msgs) {
			end = len(msgs)
		}
		pipe := p.rdb.Pipeline()
		for _, msg := range msgs[i:end] {
			body, err := json.Marshal(msg)
			if err != nil {
				return fmt.Errorf("marshaling url message: %w", err)
			}
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: FrontierStream,
				MaxLen: streamMaxLen,
				Approx: true,
				Values: map[string]interface{}{"payload": body},
			})
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("publishing url batch: %w", err)
		}
	}
	return nil
}

// StreamLen returns the number of messages in the given stream.
func (p *Publisher) StreamLen(ctx context.Context, stream string) (int64, error) {
	return p.rdb.XLen(ctx, stream).Result()
}

func (p *Publisher) Close() {}
