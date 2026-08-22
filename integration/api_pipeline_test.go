//go:build integration

package integration

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"pipes/connector/ingest"
	"pipes/pipes"
)

// TestAPIConsumer_EndToEnd_RawMessageIsPersistedToMySQLAndElasticsearch wires
// the same transformer chain main.go builds (LogKeeper -> Validator ->
// Modifier -> SQLWriter -> ESWriter) against real MySQL and Elasticsearch
// containers, and drives it through the HTTP API instead of a queue message,
// exercising the "-mode=api" code path.
func TestAPIConsumer_EndToEnd_RawMessageIsPersistedToMySQLAndElasticsearch(t *testing.T) {
	ctx := context.Background()

	db := newMySQLDB(ctx, t)
	esConnector := newESConnector(ctx, t, "integration-api-events")

	var keeperLog bytes.Buffer

	ppl := pipes.NewPipeline(ctx, testTracer(), testLogger(t)).
		AddTransformer(pipes.NewLogKeeper(&keeperLog)).
		AddTransformer(&pipes.Validator{}).
		AddTransformer(&pipes.Modifier{}).
		AddTransformer(pipes.NewSQLWriter(db)).
		AddTransformer(pipes.NewESWriter(esConnector))

	consumer := ingest.NewAPIConsumer(":0", ppl.Run)

	raw := []byte(`{"event_type":"backup.create.end","resource_type":"backup","status":"READY"}`)

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(raw))
	rec := httptest.NewRecorder()

	consumer.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	if keeperLog.String() != string(raw) {
		t.Errorf("expected LogKeeper to have written the raw message, got %q", keeperLog.String())
	}

	var count int
	row := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events")
	if err := row.Scan(&count); err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row in events table, got %d", count)
	}

	// Modifier assigns a random MessageId, so recover the one that was
	// actually persisted before checking Elasticsearch for it.
	var messageID string
	row = db.QueryRowContext(ctx, "SELECT message_id FROM events LIMIT 1")
	if err := row.Scan(&messageID); err != nil {
		t.Fatalf("failed to read persisted message_id: %v", err)
	}

	waitForDocument(ctx, t, esConnector, messageID)
}
