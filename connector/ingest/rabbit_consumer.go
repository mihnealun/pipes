package ingest

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueConsumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	Queue   string
}

func NewQueueConsumer(url, queueName string) (*QueueConsumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return &QueueConsumer{
		conn:    conn,
		channel: ch,
		Queue:   queueName,
	}, nil
}

func (c *QueueConsumer) StartConsuming() (<-chan amqp.Delivery, error) {
	return c.channel.Consume(
		c.Queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
}

func (c *QueueConsumer) Close() {
	if c.channel != nil {
		_ = c.channel.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}
