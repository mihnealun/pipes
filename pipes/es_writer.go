package pipes

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"pipes/connector/output"
	"pipes/models"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type ESWriter struct {
	next     Processor
	esWriter *output.ESWriter
	lastErr  error
}

func NewESWriter(writer *output.ESWriter) *ESWriter {
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
	t.Start(ctx, "pipes.ESWriter")

	if v.lastErr != nil {
		return fmt.Errorf("[ESWriter] no ES connection: %v", v.lastErr)
	}

	if m == nil {
		return fmt.Errorf("[ESWriter] input is empty, skipping")
	}

	message, ok := m.(models.EnrichedEvent)
	if !ok {
		return fmt.Errorf("[ESWriter] expected EnrichedEvent, got %T", m)
	}

	b, err := json.Marshal(message)
	if err != nil {
		return err
	}

	err = v.esWriter.Write(ctx, message.MessageId, b)
	if err != nil {
		return fmt.Errorf("[ESWriter] failed to save document to Elasticsearch: %v", err)
	}

	if v.next != nil {
		return v.next.Execute(ctx, t, l, message)
	}

	return nil
}

func (v *ESWriter) SetNext(t Processor) {
	v.next = t
}
