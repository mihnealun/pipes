package pipes

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"pipes/elasticsearch"
	"pipes/models"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type ESWriter struct {
	next     Processor
	esWriter *elasticsearch.ESWriter
	lastErr  error
}

func NewESWriter(writer *elasticsearch.ESWriter) *ESWriter {
	result := &ESWriter{
		esWriter: writer,
		lastErr:  nil,
	}

	go result.healthCheck()

	return result
}

func (v *ESWriter) healthCheck() {
	for range time.Tick(2 * time.Second) {
		_, v.lastErr = v.esWriter.Client.Ping()
	}
}

func (v *ESWriter) Execute(ctx context.Context, t trace.Tracer, l *log.Logger, m any) error {
	t.Start(ctx, "pipes.Writer")

	if v.lastErr != nil {
		return fmt.Errorf("no ES connection: %v", v.lastErr)
	}

	message, ok := m.(models.EnrichedEvent)
	if !ok {
		return fmt.Errorf("[Writer] expected EnrichedEvent, got %T", m)
	}

	b, err := json.Marshal(message)
	if err != nil {
		return err
	}

	err = v.esWriter.Write(ctx, message.MessageId, b)
	if err != nil {
		return fmt.Errorf("failed to save document to Elasticsearch: %v", err)
	}

	if v.next != nil {
		return v.next.Execute(ctx, t, l, message)
	}

	return nil
}

func (v *ESWriter) SetNext(t Processor) {
	v.next = t
}
