package pipes

import (
	"context"
	"errors"
	"log"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestNewPipeline(t *testing.T) {
	ctx := testContext()
	tracer := testTracer()
	logger := testLogger()

	p := NewPipeline(ctx, tracer, logger)

	if p.ctx != ctx {
		t.Errorf("expected ctx to be set")
	}
	if p.logger != logger {
		t.Errorf("expected logger to be set")
	}
	if len(p.transformers) != 0 {
		t.Errorf("expected no transformers, got %d", len(p.transformers))
	}
}

func TestPipeline_AddTransformer_ChainsProcessors(t *testing.T) {
	p := NewPipeline(testContext(), testTracer(), testLogger())

	m1 := &mockProcessor{}
	m2 := &mockProcessor{}
	m3 := &mockProcessor{}

	p.AddTransformer(m1).AddTransformer(m2).AddTransformer(m3)

	if len(p.transformers) != 3 {
		t.Fatalf("expected 3 transformers, got %d", len(p.transformers))
	}
	if m1.next != Processor(m2) {
		t.Errorf("expected m1.next to be m2")
	}
	if m2.next != Processor(m3) {
		t.Errorf("expected m2.next to be m3")
	}
	if m3.next != nil {
		t.Errorf("expected m3.next to remain nil")
	}
}

func TestPipeline_AddTransformer_NilIsNoop(t *testing.T) {
	p := NewPipeline(testContext(), testTracer(), testLogger())
	p.AddTransformer(&mockProcessor{})
	before := len(p.transformers)

	result := p.AddTransformer(nil)

	if result != p {
		t.Errorf("expected AddTransformer(nil) to return the same pipeline")
	}
	if len(p.transformers) != before {
		t.Fatalf("expected AddTransformer(nil) to be a no-op, got %d transformers", len(p.transformers))
	}
}

func TestPipeline_Run_NilInput(t *testing.T) {
	p := NewPipeline(testContext(), testTracer(), testLogger())
	p.AddTransformer(&mockProcessor{})

	if err := p.Run(nil); err == nil {
		t.Fatal("expected error for nil input")
	}
}

func TestPipeline_Run_NoTransformers(t *testing.T) {
	p := NewPipeline(testContext(), testTracer(), testLogger())

	if err := p.Run("payload"); err == nil {
		t.Fatal("expected error when no transformers are defined")
	}
}

func TestPipeline_Run_InvokesFirstTransformer(t *testing.T) {
	p := NewPipeline(testContext(), testTracer(), testLogger())
	m := &mockProcessor{}
	p.AddTransformer(m)

	err := p.Run("payload")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m.calls) != 1 || m.calls[0] != "payload" {
		t.Fatalf("expected first transformer to receive %q, got %#v", "payload", m.calls)
	}
}

func TestPipeline_Execute_ActsAsNestedTransformer(t *testing.T) {
	inner := NewPipeline(testContext(), testTracer(), testLogger())
	innerStep := &mockProcessor{}
	inner.AddTransformer(innerStep)

	outerNext := &mockProcessor{}
	inner.SetNext(outerNext)

	err := inner.Execute(testContext(), testTracer(), testLogger(), "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(innerStep.calls) != 1 || innerStep.calls[0] != "data" {
		t.Fatalf("expected inner transformer to be invoked with %q, got %#v", "data", innerStep.calls)
	}
	if len(outerNext.calls) != 1 || outerNext.calls[0] != "data" {
		t.Fatalf("expected outer next to be invoked with %q, got %#v", "data", outerNext.calls)
	}
}

func TestPipeline_Execute_InnerErrorIsLoggedButNextStillRuns(t *testing.T) {
	inner := NewPipeline(testContext(), testTracer(), testLogger())
	failing := &mockProcessor{executeFunc: func(_ context.Context, _ trace.Tracer, _ *log.Logger, _ any) error {
		return errors.New("boom")
	}}
	inner.AddTransformer(failing)

	outerNext := &mockProcessor{}
	inner.SetNext(outerNext)

	err := inner.Execute(testContext(), testTracer(), testLogger(), "data")
	if err != nil {
		t.Fatalf("expected Execute to swallow the inner pipeline error and return next's result, got: %v", err)
	}
	if len(outerNext.calls) != 1 {
		t.Fatalf("expected next to still run after inner pipeline error, got %d calls", len(outerNext.calls))
	}
}

func TestPipeline_Execute_NoNext_ReturnsNilEvenOnInnerError(t *testing.T) {
	inner := NewPipeline(testContext(), testTracer(), testLogger())
	failing := &mockProcessor{executeFunc: func(_ context.Context, _ trace.Tracer, _ *log.Logger, _ any) error {
		return errors.New("boom")
	}}
	inner.AddTransformer(failing)

	err := inner.Execute(testContext(), testTracer(), testLogger(), "data")
	if err != nil {
		t.Fatalf("expected nil error when pipeline has no next, got: %v", err)
	}
}
