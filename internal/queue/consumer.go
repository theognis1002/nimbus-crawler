package queue

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Consume(conn *Connection, queueName string) (<-chan amqp.Delivery, error) {
	deliveries, err := conn.Channel().Consume(
		queueName,
		"",    // consumer tag (auto-generated)
		false, // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("consuming from %s: %w", queueName, err)
	}
	return deliveries, nil
}
