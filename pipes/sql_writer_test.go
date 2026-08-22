package pipes

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"pipes/models"
)

// fakeSQLDriver is a minimal database/sql/driver implementation that lets
// SQLWriter tests exercise real *sql.DB Exec calls without a live MySQL
// server. It either succeeds every Exec, or fails every Exec with execErr.
type fakeSQLDriver struct {
	execErr error
}

func (d fakeSQLDriver) Open(_ string) (driver.Conn, error) {
	return &fakeSQLConn{execErr: d.execErr}, nil
}

type fakeSQLConn struct {
	execErr error
}

func (c *fakeSQLConn) Prepare(_ string) (driver.Stmt, error) {
	return &fakeSQLStmt{execErr: c.execErr}, nil
}

func (c *fakeSQLConn) Close() error { return nil }

func (c *fakeSQLConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fakeSQLConn: transactions not supported")
}

type fakeSQLStmt struct {
	execErr error
}

func (s *fakeSQLStmt) Close() error  { return nil }
func (s *fakeSQLStmt) NumInput() int { return -1 }

func (s *fakeSQLStmt) Exec(_ []driver.Value) (driver.Result, error) {
	if s.execErr != nil {
		return nil, s.execErr
	}
	return driver.RowsAffected(1), nil
}

func (s *fakeSQLStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, errors.New("fakeSQLStmt: queries not supported")
}

func init() {
	sql.Register("pipes-fake-sql-ok", fakeSQLDriver{})
	sql.Register("pipes-fake-sql-err", fakeSQLDriver{execErr: errors.New("insert failed")})
}

func newFakeDB(t *testing.T, driverName string) *sql.DB {
	t.Helper()

	db, err := sql.Open(driverName, "fake-dsn")
	if err != nil {
		t.Fatalf("sql.Open(%q): %v", driverName, err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func TestNewSQLWriter(t *testing.T) {
	db := newFakeDB(t, "pipes-fake-sql-ok")

	w := NewSQLWriter(db)

	if w.DB != db {
		t.Errorf("expected DB to be set")
	}
	if w.lastErr != nil {
		t.Errorf("expected lastErr to be nil right after construction, got %v", w.lastErr)
	}
}

func TestSQLWriter_Execute_NilInput(t *testing.T) {
	v := &SQLWriter{DB: newFakeDB(t, "pipes-fake-sql-ok")}

	if err := v.Execute(testContext(), testTracer(), testLogger(), nil); err == nil {
		t.Fatal("expected error for nil input")
	}
}

func TestSQLWriter_Execute_HealthCheckFailurePreventsWrite(t *testing.T) {
	v := &SQLWriter{
		DB:      newFakeDB(t, "pipes-fake-sql-ok"),
		lastErr: errors.New("ping failed"),
	}

	err := v.Execute(testContext(), testTracer(), testLogger(), models.EnrichedEvent{})
	if err == nil {
		t.Fatal("expected error when lastErr is set")
	}
	if !strings.Contains(err.Error(), "no SQL connection") {
		t.Errorf("expected error to mention no SQL connection, got: %v", err)
	}
}

func TestSQLWriter_Execute_WrongType(t *testing.T) {
	v := &SQLWriter{DB: newFakeDB(t, "pipes-fake-sql-ok")}

	if err := v.Execute(testContext(), testTracer(), testLogger(), "not an event"); err == nil {
		t.Fatal("expected error for non-EnrichedEvent input")
	}
}

func TestSQLWriter_Execute_InsertSucceedsAndForwards(t *testing.T) {
	v := &SQLWriter{DB: newFakeDB(t, "pipes-fake-sql-ok")}
	next := &mockProcessor{}
	v.SetNext(next)

	event := models.EnrichedEvent{MessageId: "msg-1"}

	err := v.Execute(testContext(), testTracer(), testLogger(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(next.calls) != 1 {
		t.Fatalf("expected next to be called once, got %d", len(next.calls))
	}
	got, ok := next.calls[0].(models.EnrichedEvent)
	if !ok || got.MessageId != event.MessageId {
		t.Fatalf("expected next to receive %#v, got %#v", event, next.calls[0])
	}
}

func TestSQLWriter_Execute_InsertFails(t *testing.T) {
	v := &SQLWriter{DB: newFakeDB(t, "pipes-fake-sql-err")}

	err := v.Execute(testContext(), testTracer(), testLogger(), models.EnrichedEvent{MessageId: "msg-1"})
	if err == nil {
		t.Fatal("expected error when insert fails")
	}
	if !strings.Contains(err.Error(), "failed to insert event") {
		t.Errorf("expected error to mention failed insert, got: %v", err)
	}
}

func TestSQLWriter_Execute_NoNext(t *testing.T) {
	v := &SQLWriter{DB: newFakeDB(t, "pipes-fake-sql-ok")}

	err := v.Execute(testContext(), testTracer(), testLogger(), models.EnrichedEvent{MessageId: "msg-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
