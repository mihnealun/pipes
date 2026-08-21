package pipes

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"pipes/models"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/otel/trace"
)

type SQLWriter struct {
	next    Processor
	DB      *sql.DB
	lastErr error
}

func NewSQLWriter(db *sql.DB) *SQLWriter {
	result := &SQLWriter{
		DB:      db,
		lastErr: nil,
	}

	go result.healthCheck()

	return result
}

func (v *SQLWriter) healthCheck() {
	for range time.Tick(2 * time.Second) {
		v.lastErr = v.DB.Ping()
	}
}

func (v *SQLWriter) Execute(ctx context.Context, t trace.Tracer, l *log.Logger, m any) error {
	t.Start(ctx, "pipes.SQLWriter")

	if m == nil {
		return fmt.Errorf("[SQLWriter] input is empty, skipping")
	}

	if v.lastErr != nil {
		return fmt.Errorf("[SQLWriter] no SQL connection: %v", v.lastErr)
	}

	message, ok := m.(models.EnrichedEvent)
	if !ok {
		return fmt.Errorf("[SQLWriter] expected EnrichedEvent, got %T", m)
	}

	q := `INSERT INTO events (
	  message_id, occurred_at, created_at, updated_at, processed_at, delivered_at, 
	  action, amount, event_type, os_project_id, project_id, resource_type, sku, status, stage
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := v.DB.Exec(
		q,
		message.MessageId,
		message.OccurredAt, message.CreatedAt, message.UpdatedAt, message.ProcessedAt, message.DeliveredAt,
		message.Action, message.Amount, message.EventType, message.OSProjectID, message.ProjectID,
		message.ResourceType, message.SKU, message.Status, message.Stage,
	)
	if err != nil {
		return fmt.Errorf("[SQLWriter] failed to insert event: %v", err)
	}

	if v.next != nil {
		return v.next.Execute(ctx, t, l, message)
	}

	return nil
}

func (v *SQLWriter) SetNext(t Processor) {
	v.next = t
}
