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
	ackTimeout       = 5 * time.Second
)

type Consumer struct {
	rdb      *redis.Client
	stream   string
	dlq      string
	group    string
	consumer string
	count    int
	logger   *slog.Logger
	wg       sync.WaitGroup
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

	c.wg.Add(2)
	go func() {
		defer c.wg.Done()
		c.readLoop(ctx, ch)
	}()
	go func() {
		defer c.wg.Done()
		c.reclaimLoop(ctx, ch)
	}()
	go func() {
		c.wg.Wait()
		close(ch)
	}()

	return ch
}

// Wait blocks until the consumer's internal goroutines have fully exited.
func (c *Consumer) Wait() {
	c.wg.Wait()
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
			if ctx.Err() != nil {
				return
			}
			if err == redis.Nil {
				continue
			}
			c.logger.Error("XREADGROUP error", "error", err, "stream", c.stream)
			time.Sleep(time.Second)
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				d, ok := c.buildDelivery(msg)
				if !ok {
					continue
				}
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
			d, ok := c.buildDelivery(msg)
			if !ok {
				continue
			}
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

func (c *Consumer) buildDelivery(msg redis.XMessage) (Delivery, bool) {
	payload, ok := msg.Values["payload"].(string)
	if !ok || payload == "" {
		c.logger.Error("message missing payload field", "stream", c.stream, "id", msg.ID)
		ackCtx, ackCancel := ctxBG()
		defer ackCancel()
		_ = c.rdb.XAck(ackCtx, c.stream, c.group, msg.ID).Err()
		return Delivery{}, false
	}

	id := msg.ID
	return Delivery{
		Body: []byte(payload),
		Ack: func() error {
			ctx, cancel := ctxBG()
			defer cancel()
			return c.rdb.XAck(ctx, c.stream, c.group, id).Err()
		},
		Nack: func(toDLQ bool) error {
			if toDLQ {
				addCtx, addCancel := ctxBG()
				defer addCancel()
				if err := c.rdb.XAdd(addCtx, &redis.XAddArgs{
					Stream: c.dlq,
					Values: map[string]interface{}{"payload": payload},
				}).Err(); err != nil {
					return err
				}
				ackCtx, ackCancel := ctxBG()
				defer ackCancel()
				return c.rdb.XAck(ackCtx, c.stream, c.group, id).Err()
			}
			// Requeue: no-op — message stays in PEL, reclaim loop will re-deliver it
			return nil
		},
	}, true
}

// ctxBG returns a background context with a timeout for ack/nack operations
// that must complete even after the main context is cancelled.
func ctxBG() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), ackTimeout)
}
