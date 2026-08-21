package pipes

import (
	"context"
	"fmt"
	"io"
	"log"

	"go.opentelemetry.io/otel/trace"
)

type LogKeeper struct {
	next Processor
	w    io.Writer
}

func NewLogKeeper(w io.Writer) *LogKeeper {
	return &LogKeeper{
		w: w,
	}
}

func (v *LogKeeper) Execute(ctx context.Context, t trace.Tracer, l *log.Logger, m any) error {
	t.Start(ctx, "pipes.LogKeeper")

	if m == nil {
		return fmt.Errorf("[LogKeeper] input is empty, skipping")
	}

	message, ok := m.([]byte)
	if !ok {
		return fmt.Errorf("[LogKeeper] expected []byte, got %T", m)
	}

	_, err := v.w.Write(message)
	if err != nil {
		l.Printf("[LogKeeper] failed to write message: %v", err)
	}

	if v.next != nil {
		return v.next.Execute(ctx, t, l, m)
	}

	return nil
}

func (v *LogKeeper) SetNext(t Processor) {
	v.next = t
}
