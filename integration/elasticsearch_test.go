//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	tcelasticsearch "github.com/testcontainers/testcontainers-go/modules/elasticsearch"

	"pipes/config"
	"pipes/connector/output"
	"pipes/models"
	"pipes/pipes"
)

const elasticsearchImage = "docker.elastic.co/elasticsearch/elasticsearch:7.17.18"

func newESConnector(ctx context.Context, t *testing.T, index string) *output.ESWriter {
	t.Helper()

	container, err := tcelasticsearch.Run(ctx, elasticsearchImage)
	if err != nil {
		t.Fatalf("failed to start elasticsearch container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate elasticsearch container: %v", err)
		}
	})

	cfg := &config.Config{
		ElasticsearchURL: container.Settings.Address,
		ElasticIndex:     index,
	}

	connector, err := output.NewESWriter(cfg)
	if err != nil {
		t.Fatalf("output.NewESWriter: %v", err)
	}

	return connector
}

// waitForDocument polls Elasticsearch for docID, tolerating index refresh
// latency, and fails the test if it never becomes retrievable.
func waitForDocument(ctx context.Context, t *testing.T, connector *output.ESWriter, docID string) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	var lastErr error

	for time.Now().Before(deadline) {
		res, err := connector.Client.Get(connector.Index, docID, connector.Client.Get.WithContext(ctx))
		if err == nil {
			found := !res.IsError()
			_ = res.Body.Close()
			if found {
				return
			}
			lastErr = fmt.Errorf("elasticsearch responded with status %s", res.Status())
		} else {
			lastErr = err
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("document %q was not retrievable from elasticsearch: %v", docID, lastErr)
}

// TestESWriter_Execute_IndexesIntoRealElasticsearch verifies pipes.ESWriter and
// connector/output.ESWriter against a real Elasticsearch node: a document is
// indexed and then retrievable by its message id.
func TestESWriter_Execute_IndexesIntoRealElasticsearch(t *testing.T) {
	ctx := context.Background()
	connector := newESConnector(ctx, t, "integration-events")

	writer := pipes.NewESWriter(connector)

	event := models.EnrichedEvent{
		MessageId: "integration-test-1",
		EventType: "backup.create.end",
		Status:    "READY",
		CreatedAt: time.Now().UTC(),
	}

	if err := writer.Execute(ctx, testTracer(), testLogger(t), event); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	waitForDocument(ctx, t, connector, event.MessageId)
}
