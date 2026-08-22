package pipes

import (
	"bytes"
	"errors"
	"log"
	"testing"
)

func TestNewLogKeeper(t *testing.T) {
	var buf bytes.Buffer
	lk := NewLogKeeper(&buf)

	if lk.w != &buf {
		t.Fatalf("expected writer to be set on LogKeeper")
	}
}

func TestLogKeeper_Execute_NilInput(t *testing.T) {
	lk := NewLogKeeper(&bytes.Buffer{})

	if err := lk.Execute(testContext(), testTracer(), testLogger(), nil); err == nil {
		t.Fatal("expected error for nil input")
	}
}

func TestLogKeeper_Execute_WrongType(t *testing.T) {
	lk := NewLogKeeper(&bytes.Buffer{})

	if err := lk.Execute(testContext(), testTracer(), testLogger(), "not bytes"); err == nil {
		t.Fatal("expected error for non-[]byte input")
	}
}

func TestLogKeeper_Execute_WritesAndForwardsMessage(t *testing.T) {
	var buf bytes.Buffer
	lk := NewLogKeeper(&buf)

	next := &mockProcessor{}
	lk.SetNext(next)

	msg := []byte(`{"hello":"world"}`)
	err := lk.Execute(testContext(), testTracer(), testLogger(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != string(msg) {
		t.Fatalf("writer got %q, want %q", buf.String(), msg)
	}
	if len(next.calls) != 1 {
		t.Fatalf("expected next to be called once, got %d", len(next.calls))
	}
	got, ok := next.calls[0].([]byte)
	if !ok || string(got) != string(msg) {
		t.Fatalf("next received %#v, want %q", next.calls[0], msg)
	}
}

type erroringWriter struct{}

func (erroringWriter) Write(_ []byte) (int, error) {
	return 0, errors.New("disk full")
}

func TestLogKeeper_Execute_WriteFailureIsLoggedButPipelineContinues(t *testing.T) {
	var logBuf bytes.Buffer
	logger := log.New(&logBuf, "", 0)

	lk := NewLogKeeper(erroringWriter{})
	next := &mockProcessor{}
	lk.SetNext(next)

	err := lk.Execute(testContext(), testTracer(), logger, []byte("payload"))
	if err != nil {
		t.Fatalf("expected write failure to be swallowed, got error: %v", err)
	}
	if !bytes.Contains(logBuf.Bytes(), []byte("failed to write message")) {
		t.Fatalf("expected write failure to be logged, got: %q", logBuf.String())
	}
	if len(next.calls) != 1 {
		t.Fatalf("expected next to still be called despite write failure, got %d calls", len(next.calls))
	}
}

func TestLogKeeper_Execute_NoNext(t *testing.T) {
	lk := NewLogKeeper(&bytes.Buffer{})

	if err := lk.Execute(testContext(), testTracer(), testLogger(), []byte("x")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
