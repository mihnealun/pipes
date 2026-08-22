package pipes

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"

	"pipes/connector/output"
	"pipes/models"
)

// newTestESConnector spins up a httptest server that mimics an
// Elasticsearch node closely enough for the go-elasticsearch client's
// product check (X-Elastic-Product header) to pass, and returns status
// for every request.
func newTestESConnector(t *testing.T, status int) *output.ESWriter {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	client, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{srv.URL}})
	if err != nil {
		t.Fatalf("elasticsearch.NewClient: %v", err)
	}

	return &output.ESWriter{Client: client, Index: "test-index"}
}

func TestESWriter_Execute_NilInput(t *testing.T) {
	v := &ESWriter{esWriter: newTestESConnector(t, http.StatusOK)}

	if err := v.Execute(testContext(), testTracer(), testLogger(), nil); err == nil {
		t.Fatal("expected error for nil input")
	}
}

func TestESWriter_Execute_HealthCheckFailurePreventsWrite(t *testing.T) {
	v := &ESWriter{
		esWriter: newTestESConnector(t, http.StatusOK),
		lastErr:  errors.New("ping failed"),
	}

	err := v.Execute(testContext(), testTracer(), testLogger(), models.EnrichedEvent{})
	if err == nil {
		t.Fatal("expected error when lastErr is set")
	}
	if !strings.Contains(err.Error(), "no ES connection") {
		t.Errorf("expected error to mention no ES connection, got: %v", err)
	}
}

func TestESWriter_Execute_WrongType(t *testing.T) {
	v := &ESWriter{esWriter: newTestESConnector(t, http.StatusOK)}

	if err := v.Execute(testContext(), testTracer(), testLogger(), "not an event"); err == nil {
		t.Fatal("expected error for non-EnrichedEvent input")
	}
}

func TestESWriter_Execute_WriteSucceedsAndForwards(t *testing.T) {
	v := &ESWriter{esWriter: newTestESConnector(t, http.StatusOK)}
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

func TestESWriter_Execute_WriteFails(t *testing.T) {
	v := &ESWriter{esWriter: newTestESConnector(t, http.StatusInternalServerError)}

	err := v.Execute(testContext(), testTracer(), testLogger(), models.EnrichedEvent{MessageId: "msg-1"})
	if err == nil {
		t.Fatal("expected error when Elasticsearch returns an error status")
	}
	if !strings.Contains(err.Error(), "failed to save document to Elasticsearch") {
		t.Errorf("expected error to mention failed save, got: %v", err)
	}
}

func TestESWriter_Execute_NoNext(t *testing.T) {
	v := &ESWriter{esWriter: newTestESConnector(t, http.StatusOK)}

	err := v.Execute(testContext(), testTracer(), testLogger(), models.EnrichedEvent{MessageId: "msg-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
