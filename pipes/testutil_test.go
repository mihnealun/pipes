package pipes

import (
	"context"
	"io"
	"log"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func testContext() context.Context {
	return context.Background()
}

func testTracer() trace.Tracer {
	return noop.NewTracerProvider().Tracer("test")
}

func testLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

// mockProcessor is a Processor test double that records every input it
// receives and optionally delegates to a caller-supplied executeFunc.
type mockProcessor struct {
	executeFunc func(ctx context.Context, t trace.Tracer, l *log.Logger, m any) error
	calls       []any
	next        Processor
}

func (m *mockProcessor) Execute(ctx context.Context, t trace.Tracer, l *log.Logger, in any) error {
	m.calls = append(m.calls, in)
	if m.executeFunc != nil {
		return m.executeFunc(ctx, t, l, in)
	}
	return nil
}

func (m *mockProcessor) SetNext(t Processor) {
	m.next = t
}
