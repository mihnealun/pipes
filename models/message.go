package models

type Message struct {
	EventID      string `json:"event_id"`
	EventType    string `json:"event_type"`
	ResourceType string `json:"resource_type"`
	Status       string `json:"status"`
}
