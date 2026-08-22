//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"pipes/connector/ingest"
)

// TestQueueConsumer_ConsumesPublishedMessage verifies connector/ingest.QueueConsumer
// against a real RabbitMQ broker: it declares the queue, and a message published by
// an independent producer connection is delivered to StartConsuming's channel.
func TestQueueConsumer_ConsumesPublishedMessage(t *testing.T) {
	ctx := context.Background()

	container, err := rabbitmq.Run(ctx, "rabbitmq:3.12.11-management-alpine")
	if err != nil {
		t.Fatalf("failed to start rabbitmq container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate rabbitmq container: %v", err)
		}
	})

	url, err := container.AmqpURL(ctx)
	if err != nil {
		t.Fatalf("failed to resolve amqp url: %v", err)
	}

	const queueName = "integration-events"

	consumer, err := ingest.NewQueueConsumer(url, queueName)
	if err != nil {
		t.Fatalf("NewQueueConsumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	messages, err := consumer.StartConsuming()
	if err != nil {
		t.Fatalf("StartConsuming: %v", err)
	}

	producerConn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("amqp.Dial (producer): %v", err)
	}
	t.Cleanup(func() { _ = producerConn.Close() })

	producerCh, err := producerConn.Channel()
	if err != nil {
		t.Fatalf("producer Channel: %v", err)
	}
	t.Cleanup(func() { _ = producerCh.Close() })

	body := []byte(`{"event_type":"backup.create.end","resource_type":"backup","status":"READY"}`)

	err = producerCh.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case d := <-messages:
		if string(d.Body) != string(body) {
			t.Fatalf("got body %q, want %q", d.Body, body)
		}
		if err := d.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for published message to be consumed")
	}
}
