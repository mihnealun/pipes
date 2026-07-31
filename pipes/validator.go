package pipes

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"

	"go.opentelemetry.io/otel/trace"
)

var (
	eventTypes = []string{
		"compute.instance.create.end",
		"compute.instance.delete.start",
		"volume.delete.start",
		"floatingip.delete.end",
		"volume.create.end",
		"floatingip.create.end",
		"backup.create.end",
		"snapshot.create.end",
		"backup.delete.end",
		"snapshot.delete.start",
		"volume.resize.end",
		"compute.instance.shelve_offload.start",
		"router.create.end",
		"compute.instance.resize.end",
		"compute.instance.unshelve.end",
		"router.delete.end",
		"compute.instance.resize.confirm.end",
		"image.delete",
		"already.fixed.volume.type.sku.create",
		"image.activate",
		"compute.instance.finish_resize.end",
	}

	resourceTypes = []string{
		"backup",
		"image",
		"license",
		"publicip",
		"router",
		"server",
		"snapshot",
		"volume-amount",
		"volume-type",
	}

	actions = []string{
		"START",
		"STOP",
		"CHANGE",
	}

	stages = []string{
		"stage",
		"prod",
		"qa",
	}

	statuses = []string{
		"DELIVERED",
		"ERROR",
		"READY",
		"VALIDATION_FAILED",
	}
)

type Validator struct {
	next Processor
}

func (v *Validator) Execute(ctx context.Context, t trace.Tracer, l *log.Logger, m any) error {
	t.Start(ctx, "pipes.Validator")
	message, ok := m.([]byte)
	if !ok {
		return fmt.Errorf("[Validator] expected []byte, got %T", m)
	}

	var rawEvent map[string]any
	if err := json.Unmarshal(message, &rawEvent); err != nil {
		return err
	}

	if !v.isValidMessage(rawEvent) {
		return fmt.Errorf("invalid message")
	}

	if v.next != nil {
		return v.next.Execute(ctx, t, l, rawEvent)
	}

	return nil
}

func (v *Validator) SetNext(t Processor) {
	v.next = t
}

// isValidMessage - only whitelisted messages
func (v *Validator) isValidMessage(m map[string]any) bool {
	if m == nil {
		return false
	}

	if !slices.Contains(eventTypes, m["event_type"].(string)) {
		return false
	}

	if !slices.Contains(resourceTypes, m["resource_type"].(string)) {
		return false
	}

	if !slices.Contains(statuses, m["status"].(string)) {
		return false
	}

	return true
}
