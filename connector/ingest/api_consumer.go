package ingest

import (
	"context"
	"io"
	"net/http"

	"github.com/labstack/echo/v4"
)

// PipelineRunner runs a single message through the processing pipeline. It
// matches the signature of pipes.Pipeline.Run, so a pipeline built once at
// startup can be reused as-is to drive either ingestion mode.
type PipelineRunner func(m any) error

// APIConsumer exposes an HTTP endpoint (built with the Echo framework) that
// accepts raw event payloads and runs each one through the pipeline
// synchronously, so the caller's response reflects the pipeline's own
// outcome instead of just an "enqueued" acknowledgement.
type APIConsumer struct {
	echo *echo.Echo
	addr string
}

// NewAPIConsumer wires a POST /events endpoint that hands the raw request
// body to run, and a GET /healthz endpoint for liveness checks.
func NewAPIConsumer(addr string, run PipelineRunner) *APIConsumer {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	c := &APIConsumer{echo: e, addr: addr}

	e.POST("/events", func(ec echo.Context) error {
		body, err := io.ReadAll(ec.Request().Body)
		if err != nil {
			return ec.JSON(http.StatusBadRequest, echo.Map{"error": "failed to read request body"})
		}

		if len(body) == 0 {
			return ec.JSON(http.StatusBadRequest, echo.Map{"error": "empty request body"})
		}

		if err := run(body); err != nil {
			return ec.JSON(http.StatusUnprocessableEntity, echo.Map{"error": err.Error()})
		}

		return ec.JSON(http.StatusAccepted, echo.Map{"status": "accepted"})
	})

	e.GET("/healthz", func(ec echo.Context) error {
		return ec.NoContent(http.StatusOK)
	})

	return c
}

// Start blocks, serving the API until the server is shut down.
func (c *APIConsumer) Start() error {
	return c.echo.Start(c.addr)
}

// ServeHTTP lets APIConsumer be driven directly, e.g. from tests via
// net/http/httptest, without binding a real network listener.
func (c *APIConsumer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.echo.ServeHTTP(w, r)
}

// Shutdown gracefully stops the API server.
func (c *APIConsumer) Shutdown(ctx context.Context) error {
	return c.echo.Shutdown(ctx)
}
