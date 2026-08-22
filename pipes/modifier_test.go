package pipes

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"pipes/models"
)

func TestModifier_Execute_NilInput(t *testing.T) {
	v := &Modifier{}

	if err := v.Execute(testContext(), testTracer(), testLogger(), nil); err == nil {
		t.Fatal("expected error for nil input")
	}
}

func TestModifier_Execute_WrongType(t *testing.T) {
	v := &Modifier{}

	if err := v.Execute(testContext(), testTracer(), testLogger(), "not a map"); err == nil {
		t.Fatal("expected error for non-map[string]any input")
	}
}

func TestModifier_Execute_ProducesEnrichedEvent(t *testing.T) {
	next := &mockProcessor{}
	v := &Modifier{}
	v.SetNext(next)

	input := map[string]any{"event_id": "abc-123"}

	before := time.Now().UTC()
	err := v.Execute(testContext(), testTracer(), testLogger(), input)
	after := time.Now().UTC()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(next.calls) != 1 {
		t.Fatalf("expected next to be called once, got %d", len(next.calls))
	}

	ev, ok := next.calls[0].(models.EnrichedEvent)
	if !ok {
		t.Fatalf("expected next to receive models.EnrichedEvent, got %T", next.calls[0])
	}

	if _, err := uuid.Parse(ev.MessageId); err != nil {
		t.Errorf("expected MessageId to be a valid UUID, got %q: %v", ev.MessageId, err)
	}
	if _, err := uuid.Parse(ev.OSProjectID); err != nil {
		t.Errorf("expected OSProjectID to be a valid UUID, got %q: %v", ev.OSProjectID, err)
	}
	if _, err := uuid.Parse(ev.ProjectID); err != nil {
		t.Errorf("expected ProjectID to be a valid UUID, got %q: %v", ev.ProjectID, err)
	}
	if _, err := uuid.Parse(ev.ResourceID); err != nil {
		t.Errorf("expected ResourceID to be a valid UUID, got %q: %v", ev.ResourceID, err)
	}
	if _, err := uuid.Parse(ev.TraceParent); err != nil {
		t.Errorf("expected TraceParent to be a valid UUID, got %q: %v", ev.TraceParent, err)
	}

	if !reflect.DeepEqual(ev.Payload, input) {
		t.Errorf("expected Payload to equal input, got %#v, want %#v", ev.Payload, input)
	}

	if ev.Amount != 20 {
		t.Errorf("expected Amount to be 20, got %d", ev.Amount)
	}
	if ev.PushFailureCount != 0 {
		t.Errorf("expected PushFailureCount to be 0, got %d", ev.PushFailureCount)
	}

	if !slices.Contains(actions, ev.Action) {
		t.Errorf("expected Action %q to be one of %v", ev.Action, actions)
	}
	if !slices.Contains(eventTypes, ev.EventType) {
		t.Errorf("expected EventType %q to be one of %v", ev.EventType, eventTypes)
	}
	if !slices.Contains(resourceTypes, ev.ResourceType) {
		t.Errorf("expected ResourceType %q to be one of %v", ev.ResourceType, resourceTypes)
	}
	if !slices.Contains(statuses, ev.Status) {
		t.Errorf("expected Status %q to be one of %v", ev.Status, statuses)
	}
	if !slices.Contains(stages, ev.Stage) {
		t.Errorf("expected Stage %q to be one of %v", ev.Stage, stages)
	}

	if ev.CreatedAt.Before(before) || ev.CreatedAt.After(after) {
		t.Errorf("expected CreatedAt %v to be within [%v, %v]", ev.CreatedAt, before, after)
	}

	wantOccurredLow := before.Add(-10 * time.Minute)
	wantOccurredHigh := after.Add(-10 * time.Minute)
	if ev.OccurredAt.Before(wantOccurredLow) || ev.OccurredAt.After(wantOccurredHigh) {
		t.Errorf("expected OccurredAt %v to be within [%v, %v]", ev.OccurredAt, wantOccurredLow, wantOccurredHigh)
	}
}

func TestModifier_Execute_NoNext(t *testing.T) {
	v := &Modifier{}

	if err := v.Execute(testContext(), testTracer(), testLogger(), map[string]any{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
