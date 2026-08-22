//go:build integration

package integration

import (
	"bytes"
	"context"
	"testing"

	"pipes/pipes"
)

// TestPipeline_EndToEnd_RawMessageIsPersistedToMySQLAndElasticsearch wires the
// same transformer chain main.go builds (LogKeeper -> Validator -> Modifier ->
// SQLWriter -> ESWriter) against real MySQL and Elasticsearch containers, and
// drives it with a raw JSON message the way a RabbitMQ delivery body would.
func TestPipeline_EndToEnd_RawMessageIsPersistedToMySQLAndElasticsearch(t *testing.T) {
	ctx := context.Background()

	db := newMySQLDB(ctx, t)
	esConnector := newESConnector(ctx, t, "integration-pipeline-events")

	var keeperLog bytes.Buffer

	ppl := pipes.NewPipeline(ctx, testTracer(), testLogger(t)).
		AddTransformer(pipes.NewLogKeeper(&keeperLog)).
		AddTransformer(&pipes.Validator{}).
		AddTransformer(&pipes.Modifier{}).
		AddTransformer(pipes.NewSQLWriter(db)).
		AddTransformer(pipes.NewESWriter(esConnector))

	raw := []byte(`{"event_type":"backup.create.end","resource_type":"backup","status":"READY"}`)

	if err := ppl.Run(raw); err != nil {
		t.Fatalf("pipeline run: %v", err)
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
