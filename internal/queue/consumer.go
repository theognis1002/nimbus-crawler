package queue

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	blockDuration    = 5 * time.Second
	reclaimInterval  = 30 * time.Second
	reclaimMinIdle   = 60 * time.Second
	reclaimBatchSize = 50
)

type Consumer struct {
	rdb      *redis.Client
	stream   string
	dlq      string
	group    string
	consumer string
	count    int
	logger   *slog.Logger
}

func NewConsumer(rdb *redis.Client, stream, dlq, group, consumerName string, count int, logger *slog.Logger) *Consumer {
	return &Consumer{
		rdb:      rdb,
		stream:   stream,
		dlq:      dlq,
		group:    group,
		consumer: consumerName,
		count:    count,
		logger:   logger,
	}
}

// Run starts reading from the stream and returns a channel of Delivery.
// The channel is closed when ctx is cancelled and both loops exit.
func (c *Consumer) Run(ctx context.Context) <-chan Delivery {
	ch := make(chan Delivery)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.readLoop(ctx, ch)
	}()
	go func() {
		defer wg.Done()
		c.reclaimLoop(ctx, ch)
	}()
	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch
}

func (c *Consumer) readLoop(ctx context.Context, ch chan<- Delivery) {
	for {
		if ctx.Err() != nil {
			return
		}

		streams, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.consumer,
			Streams:  []string{c.stream, ">"},
			Count:    int64(c.count),
			Block:    blockDuration,
		}).Result()

		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue
			}
			c.logger.Error("XREADGROUP error", "error", err, "stream", c.stream)
			time.Sleep(time.Second)
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				d := c.buildDelivery(msg)
				select {
				case ch <- d:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (c *Consumer) reclaimLoop(ctx context.Context, ch chan<- Delivery) {
	ticker := time.NewTicker(reclaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reclaimPending(ctx, ch)
		}
	}
}

func (c *Consumer) reclaimPending(ctx context.Context, ch chan<- Delivery) {
	start := "0-0"
	for {
		msgs, newStart, err := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   c.stream,
			Group:    c.group,
			Consumer: c.consumer,
			MinIdle:  reclaimMinIdle,
			Start:    start,
			Count:    reclaimBatchSize,
		}).Result()

		if err != nil {
			if ctx.Err() == nil {
				c.logger.Error("XAUTOCLAIM error", "error", err, "stream", c.stream)
			}
			return
		}

		for _, msg := range msgs {
			d := c.buildDelivery(msg)
			select {
			case ch <- d:
			case <-ctx.Done():
				return
			}
		}

		if newStart == "0-0" || len(msgs) == 0 {
			break
		}
		start = newStart
	}
}

func (c *Consumer) buildDelivery(msg redis.XMessage) Delivery {
	payload, _ := msg.Values["payload"].(string)

	id := msg.ID
	return Delivery{
		Body: []byte(payload),
		Ack: func() error {
			return c.rdb.XAck(ctxBG(), c.stream, c.group, id).Err()
		},
		Nack: func(toDLQ bool) error {
			if toDLQ {
				if err := c.rdb.XAdd(ctxBG(), &redis.XAddArgs{
					Stream: c.dlq,
					Values: map[string]interface{}{"payload": payload},
				}).Err(); err != nil {
					return err
				}
				return c.rdb.XAck(ctxBG(), c.stream, c.group, id).Err()
			}
			// Requeue: no-op — message stays in PEL, reclaim loop will re-deliver it
			return nil
		},
	}
}

// ctxBG returns a background context for ack/nack operations
// that must complete even after the main context is cancelled.
func ctxBG() context.Context {
	return context.Background()
}
