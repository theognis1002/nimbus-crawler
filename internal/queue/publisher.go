package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	mu sync.Mutex
	ch *amqp.Channel
}

func NewPublisher(conn *Connection) (*Publisher, error) {
	ch, err := conn.NewPublishChannel()
	if err != nil {
		return nil, err
	}
	return &Publisher{ch: ch}, nil
}

func (p *Publisher) PublishURL(ctx context.Context, msg URLMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling url message: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return Publish(ctx, p.ch, RoutingKeyCrawl, body)
}

func (p *Publisher) PublishParse(ctx context.Context, msg ParseMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling parse message: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return Publish(ctx, p.ch, RoutingKeyParse, body)
}

func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil {
		p.ch.Close()
	}
}
