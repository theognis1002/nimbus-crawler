package queue

import (
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeName    = "nimbus.topic"
	DLXExchangeName = "nimbus.dlx"

	FrontierQueue = "frontier_queue"
	ParseQueue    = "parse_queue"
	FrontierDLQ   = "frontier_dlq"
	ParseDLQ      = "parse_dlq"

	RoutingKeyCrawl = "url.crawl"
	RoutingKeyParse = "url.parse"
)

type Connection struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	logger  *slog.Logger
}

func NewConnection(url string, logger *slog.Logger) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("dialing rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("opening channel: %w", err)
	}

	c := &Connection{conn: conn, channel: ch, logger: logger}
	if err := c.declareTopology(); err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

func (c *Connection) declareTopology() error {
	if err := c.channel.ExchangeDeclare(ExchangeName, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring exchange %s: %w", ExchangeName, err)
	}

	if err := c.channel.ExchangeDeclare(DLXExchangeName, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring DLX exchange: %w", err)
	}

	dlArgs := amqp.Table{"x-dead-letter-exchange": DLXExchangeName}

	if _, err := c.channel.QueueDeclare(FrontierQueue, true, false, false, false, dlArgs); err != nil {
		return fmt.Errorf("declaring frontier queue: %w", err)
	}
	if err := c.channel.QueueBind(FrontierQueue, RoutingKeyCrawl, ExchangeName, false, nil); err != nil {
		return fmt.Errorf("binding frontier queue: %w", err)
	}

	if _, err := c.channel.QueueDeclare(ParseQueue, true, false, false, false, dlArgs); err != nil {
		return fmt.Errorf("declaring parse queue: %w", err)
	}
	if err := c.channel.QueueBind(ParseQueue, RoutingKeyParse, ExchangeName, false, nil); err != nil {
		return fmt.Errorf("binding parse queue: %w", err)
	}

	if _, err := c.channel.QueueDeclare(FrontierDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring frontier DLQ: %w", err)
	}
	if err := c.channel.QueueBind(FrontierDLQ, RoutingKeyCrawl, DLXExchangeName, false, nil); err != nil {
		return fmt.Errorf("binding frontier DLQ: %w", err)
	}

	if _, err := c.channel.QueueDeclare(ParseDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring parse DLQ: %w", err)
	}
	if err := c.channel.QueueBind(ParseDLQ, RoutingKeyParse, DLXExchangeName, false, nil); err != nil {
		return fmt.Errorf("binding parse DLQ: %w", err)
	}

	return nil
}

func (c *Connection) Channel() *amqp.Channel {
	return c.channel
}

func (c *Connection) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Connection) NotifyClose() chan *amqp.Error {
	return c.conn.NotifyClose(make(chan *amqp.Error, 1))
}

// SetPrefetch sets QoS prefetch count on the channel.
func (c *Connection) SetPrefetch(count int) error {
	return c.channel.Qos(count, 0, false)
}

// NewPublishChannel opens a new channel for publishing (separate from consume channel).
func (c *Connection) NewPublishChannel() (*amqp.Channel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("opening publish channel: %w", err)
	}
	return ch, nil
}

// Publish publishes a message. Use context for timeout.
func Publish(ctx context.Context, ch *amqp.Channel, routingKey string, body []byte) error {
	return ch.PublishWithContext(ctx, ExchangeName, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}
