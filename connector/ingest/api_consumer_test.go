package ingest

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIConsumer_Events_Accepted(t *testing.T) {
	var gotBody string

	c := NewAPIConsumer(":0", func(m any) error {
		body, ok := m.([]byte)
		if !ok {
			t.Fatalf("expected []byte, got %T", m)
		}
		gotBody = string(body)
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(`{"event_type":"image.delete"}`))
	rec := httptest.NewRecorder()

	c.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}

	if gotBody != `{"event_type":"image.delete"}` {
		t.Fatalf("unexpected body passed to runner: %q", gotBody)
	}
}

func TestAPIConsumer_Events_PipelineError(t *testing.T) {
	c := NewAPIConsumer(":0", func(m any) error {
		return errors.New("invalid message")
	})

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()

	c.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected status %d, got %d", http.StatusUnprocessableEntity, rec.Code)
	}
}

func TestAPIConsumer_Events_EmptyBody(t *testing.T) {
	called := false

	c := NewAPIConsumer(":0", func(m any) error {
		called = true
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(``))
	rec := httptest.NewRecorder()

	c.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}

	if called {
		t.Fatal("expected runner not to be called for empty body")
	}
}

func TestAPIConsumer_Healthz(t *testing.T) {
	c := NewAPIConsumer(":0", func(m any) error { return nil })

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	c.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
}
