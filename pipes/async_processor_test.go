package pipes

import (
	"context"
	"errors"
	"log"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestAsyncProcessor_Execute_NilInput(t *testing.T) {
	v := &AsyncProcessor{pipes: map[string]Processor{}}

	if err := v.Execute(testContext(), testTracer(), testLogger(), nil); err == nil {
		t.Fatal("expected error for nil input")
	}
}

func TestAsyncProcessor_AddTransformer_RegistersBranch(t *testing.T) {
	v := &AsyncProcessor{pipes: map[string]Processor{}}
	m := &mockProcessor{}

	result := v.AddTransformer("branch-a", m)

	if result != v {
		t.Errorf("expected AddTransformer to return the same AsyncProcessor")
	}
	if v.pipes["branch-a"] != Processor(m) {
		t.Fatalf("expected branch-a to be registered")
	}
}

func TestAsyncProcessor_Execute_RunsAllBranchesAndForwardsResults(t *testing.T) {
	v := &AsyncProcessor{pipes: map[string]Processor{}}

	branchErr := errors.New("branch failure")
	okBranch := &mockProcessor{}
	failBranch := &mockProcessor{executeFunc: func(_ context.Context, _ trace.Tracer, _ *log.Logger, _ any) error {
		return branchErr
	}}

	v.AddTransformer("ok", okBranch).AddTransformer("fail", failBranch)

	next := &mockProcessor{}
	v.SetNext(next)

	err := v.Execute(testContext(), testTracer(), testLogger(), "input")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(okBranch.calls) != 1 || okBranch.calls[0] != "input" {
		t.Fatalf("expected ok branch to receive input, got %#v", okBranch.calls)
	}
	if len(failBranch.calls) != 1 || failBranch.calls[0] != "input" {
		t.Fatalf("expected fail branch to receive input, got %#v", failBranch.calls)
	}

	if len(next.calls) != 1 {
		t.Fatalf("expected next to be called once, got %d", len(next.calls))
	}
	result, ok := next.calls[0].(map[string]any)
	if !ok {
		t.Fatalf("expected next to receive map[string]any, got %T", next.calls[0])
	}
	if result["ok"] != nil {
		t.Errorf("expected ok branch result to be nil, got %v", result["ok"])
	}
	failResult, _ := result["fail"].(error)
	if !errors.Is(failResult, branchErr) {
		t.Errorf("expected fail branch result to be %v, got %v", branchErr, result["fail"])
	}
}

func TestAsyncProcessor_Execute_NoNext(t *testing.T) {
	v := &AsyncProcessor{pipes: map[string]Processor{}}
	v.AddTransformer("a", &mockProcessor{})

	if err := v.Execute(testContext(), testTracer(), testLogger(), "input"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
