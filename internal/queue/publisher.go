package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const streamMaxLen int64 = 100000

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

func (p *Publisher) Close() {}
