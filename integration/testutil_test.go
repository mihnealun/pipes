//go:build integration

package integration

import (
	"log"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func testTracer() trace.Tracer {
	return noop.NewTracerProvider().Tracer("integration")
}

// testLogger routes *log.Logger output through t.Log so it shows up
// interleaved with test output under `go test -v`.
func testLogger(t *testing.T) *log.Logger {
	t.Helper()
	return log.New(testWriter{t}, "", 0)
}

type testWriter struct {
	t *testing.T
}

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
