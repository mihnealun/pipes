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

func (p *Pipeline) Execute(m any) error {
	p.tracer.Start(p.ctx, "pipeline")
	if len(p.transformers) == 0 {
		return fmt.Errorf("[Pipeline] no transformers defined")
	}

	return p.transformers[0].Execute(p.ctx, p.tracer, p.logger, m)
}
