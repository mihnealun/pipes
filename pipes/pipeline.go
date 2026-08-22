package pipes

import (
	"context"
	"fmt"
	"log"

	"go.opentelemetry.io/otel/trace"
)

type Processor interface {
	Execute(ctx context.Context, t trace.Tracer, l *log.Logger, m any) error
	SetNext(t Processor)
}

type Pipeline struct {
	transformers []Processor
	logger       *log.Logger
	tracer       trace.Tracer
	ctx          context.Context
	next         Processor
}

func NewPipeline(ctx context.Context, tracer trace.Tracer, logger *log.Logger) *Pipeline {
	return &Pipeline{
		transformers: make([]Processor, 0),
		logger:       logger,
		tracer:       tracer,
		ctx:          ctx,
	}
}

func (p *Pipeline) AddTransformer(t Processor) *Pipeline {
	if t == nil {
		return p
	}

	if len(p.transformers) == 0 {
		p.transformers = append(p.transformers, t)
		return p
	}

	p.transformers[len(p.transformers)-1].SetNext(t)
	p.transformers = append(p.transformers, t)

	return p
}

// Run runs as an independent pipeline
func (p *Pipeline) Run(m any) error {
	p.tracer.Start(p.ctx, "pipeline")

	if m == nil {
		return fmt.Errorf("[Pipeline] input is empty, skipping")
	}

	if len(p.transformers) == 0 {
		return fmt.Errorf("[Pipeline] no transformers defined")
	}

	return p.transformers[0].Execute(p.ctx, p.tracer, p.logger, m)
}

// SetNext for the transformer use, not use in "independent pipeline" mode
func (p *Pipeline) SetNext(t Processor) {
	p.next = t
}

// Execute runs as a transformer
func (p *Pipeline) Execute(_ context.Context, _ trace.Tracer, _ *log.Logger, m any) error {
	err := p.Run(m)
	if err != nil {
		p.logger.Printf("[Pipeline transformer] error in pipeline: %v", err)
	}

	// if part of a parent pipeline, run next
	if p.next != nil {
		return p.next.Execute(p.ctx, p.tracer, p.logger, m)
	}

	return nil
}
