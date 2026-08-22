package pipes

import (
	"context"
	"fmt"
	"log"
	"sync"

	"go.opentelemetry.io/otel/trace"
)

type AsyncProcessor struct {
	next  Processor
	pipes map[string]Processor
}

func (v *AsyncProcessor) Execute(ctx context.Context, t trace.Tracer, l *log.Logger, m any) error {
	t.Start(ctx, "pipes.AsyncProcessor")

	if m == nil {
		return fmt.Errorf("[AsyncProcessor] input is empty, skipping")
	}

	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	result := make(map[string]any)

	for n, p := range v.pipes {
		wg.Go(func() {
			err := p.Execute(ctx, t, l, m)

			mu.Lock()
			result[n] = err
			mu.Unlock()
		})
	}

	wg.Wait()

	if v.next != nil {
		return v.next.Execute(ctx, t, l, result)
	}

	return nil
}

func (v *AsyncProcessor) AddTransformer(name string, t Processor) *AsyncProcessor {
	v.pipes[name] = t

	return v
}

func (v *AsyncProcessor) SetNext(t Processor) {
	v.next = t
}
