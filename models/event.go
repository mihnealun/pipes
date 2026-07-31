package models

import (
	"time"
)

type EnrichedEvent struct {
	MessageId        string         `json:"message_id"`
	Payload          map[string]any `json:"payload"`
	OccurredAt       time.Time      `json:"occurred_at"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
	ProcessedAt      time.Time      `json:"processed_at"`
	DeliveredAt      time.Time      `json:"delivered_at"`
	Action           string         `json:"action"`
	Amount           int64          `json:"amount"`
	EventType        string         `json:"event_type"`
	OSProjectID      string         `json:"os_project_id"`
	ProjectID        string         `json:"project_id"`
	ResourceID       string         `json:"resource_id"`
	ResourceType     string         `json:"resource_type"`
	SKU              string         `json:"sku"`
	Status           string         `json:"status"`
	Stage            string         `json:"stage"`
	TraceParent      string         `json:"trace_parent"`
	PushFailureCount int            `json:"push_failure_count"`
	PushFirstFailAt  time.Time      `json:"push_first_failure_at"`
}
