package pipes

import (
	"context"
	"fmt"
	"log"
	rand2 "math/rand"
	"pipes/models"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

type Modifier struct {
	next Processor
}

func (v *Modifier) Execute(ctx context.Context, t trace.Tracer, l *log.Logger, m any) error {
	t.Start(ctx, "pipes.modifier")

	if m == nil {
		return fmt.Errorf("[Modifier] input is empty, skipping")
	}

	message, ok := m.(map[string]any)
	if !ok {
		return fmt.Errorf("[Modifier] expected map[string]any, got %T", m)
	}

	rand2.Seed(time.Now().UnixNano())
	var amount int64 = 20

	enriched := models.EnrichedEvent{
		MessageId:        uuid.New().String(),
		Payload:          message,
		OccurredAt:       time.Now().UTC().Add(-10 * time.Minute),
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
		ProcessedAt:      time.Now().UTC(),
		DeliveredAt:      time.Now().UTC(),
		Action:           actions[rand2.Intn(len(actions)-1)],
		Amount:           amount,
		EventType:        eventTypes[rand2.Intn(len(eventTypes)-1)],
		OSProjectID:      uuid.New().String(),
		ProjectID:        uuid.New().String(),
		ResourceID:       uuid.New().String(),
		ResourceType:     resourceTypes[rand2.Intn(len(resourceTypes)-1)],
		SKU:              fmt.Sprintf("SKU-%d", rand2.Intn(99)),
		Status:           statuses[rand2.Intn(len(statuses)-1)],
		Stage:            stages[rand2.Intn(len(stages)-1)],
		TraceParent:      uuid.New().String(),
		PushFailureCount: 0,
	}

	if v.next != nil {
		return v.next.Execute(ctx, t, l, enriched)
	}

	return nil
}

func (v *Modifier) SetNext(t Processor) {
	v.next = t
}
