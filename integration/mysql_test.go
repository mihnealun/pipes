//go:build integration

package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"

	"pipes/models"
	"pipes/pipes"
)

// eventsSchema mirrors the columns pipes.SQLWriter inserts into (see
// pipes/sql_writer.go's INSERT statement). Shared with pipeline_test.go.
const eventsSchema = `
CREATE TABLE events (
  message_id VARCHAR(64) PRIMARY KEY,
  occurred_at DATETIME NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  processed_at DATETIME NOT NULL,
  delivered_at DATETIME NOT NULL,
  action VARCHAR(32) NOT NULL,
  amount BIGINT NOT NULL,
  event_type VARCHAR(128) NOT NULL,
  os_project_id VARCHAR(64) NOT NULL,
  project_id VARCHAR(64) NOT NULL,
  resource_type VARCHAR(64) NOT NULL,
  sku VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  stage VARCHAR(32) NOT NULL
)`

func newMySQLDB(ctx context.Context, t *testing.T) *sql.DB {
	t.Helper()

	container, err := tcmysql.Run(ctx, "mysql:8.0.36")
	if err != nil {
		t.Fatalf("failed to start mysql container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate mysql container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		t.Fatalf("ConnectionString: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.ExecContext(ctx, eventsSchema); err != nil {
		t.Fatalf("failed to create events table: %v", err)
	}

	return db
}

// TestSQLWriter_Execute_InsertsIntoRealMySQL verifies pipes.SQLWriter against a
// real MySQL server: a valid EnrichedEvent is inserted and readable back.
func TestSQLWriter_Execute_InsertsIntoRealMySQL(t *testing.T) {
	ctx := context.Background()
	db := newMySQLDB(ctx, t)

	writer := pipes.NewSQLWriter(db)

	event := models.EnrichedEvent{
		MessageId:    "integration-test-1",
		OccurredAt:   time.Now().UTC(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
		ProcessedAt:  time.Now().UTC(),
		DeliveredAt:  time.Now().UTC(),
		Action:       "START",
		Amount:       20,
		EventType:    "backup.create.end",
		OSProjectID:  "os-project-1",
		ProjectID:    "project-1",
		ResourceType: "backup",
		SKU:          "SKU-1",
		Status:       "READY",
		Stage:        "qa",
	}

	if err := writer.Execute(ctx, testTracer(), testLogger(t), event); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var gotStatus, gotEventType string
	row := db.QueryRowContext(ctx, "SELECT status, event_type FROM events WHERE message_id = ?", event.MessageId)
	if err := row.Scan(&gotStatus, &gotEventType); err != nil {
		t.Fatalf("failed to read back inserted row: %v", err)
	}
	if gotStatus != event.Status {
		t.Errorf("got status %q, want %q", gotStatus, event.Status)
	}
	if gotEventType != event.EventType {
		t.Errorf("got event_type %q, want %q", gotEventType, event.EventType)
	}
}

// TestSQLWriter_Execute_DuplicateMessageIdFails exercises the failure path
// against a real server: the events table primary key is message_id, so a
// second insert with the same id must surface as an error from Execute.
func TestSQLWriter_Execute_DuplicateMessageIdFails(t *testing.T) {
	ctx := context.Background()
	db := newMySQLDB(ctx, t)

	writer := pipes.NewSQLWriter(db)

	now := time.Now().UTC()
	event := models.EnrichedEvent{
		MessageId:    "duplicate-id",
		Action:       "START",
		EventType:    "backup.create.end",
		ResourceType: "backup",
		Status:       "READY",
		Stage:        "qa",
		OccurredAt:   now,
		CreatedAt:    now,
		UpdatedAt:    now,
		ProcessedAt:  now,
		DeliveredAt:  now,
	}

	if err := writer.Execute(ctx, testTracer(), testLogger(t), event); err != nil {
		t.Fatalf("first insert: unexpected error: %v", err)
	}

	if err := writer.Execute(ctx, testTracer(), testLogger(t), event); err == nil {
		t.Fatal("expected second insert with the same message_id to fail")
	}
}
