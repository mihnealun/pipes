package output

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"pipes/config"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type ESWriter struct {
	Client *elasticsearch.Client
	Index  string
}

func NewESWriter(cfg *config.Config) (*ESWriter, error) {
	esCfg := elasticsearch.Config{
		Addresses: []string{cfg.ElasticsearchURL},
	}

	// Apply Authentication Strategy
	if cfg.ElasticAPIKey != "" {
		esCfg.APIKey = cfg.ElasticAPIKey
	} else if cfg.ElasticUsername != "" && cfg.ElasticPassword != "" {
		esCfg.Username = cfg.ElasticUsername
		esCfg.Password = cfg.ElasticPassword
		esCfg.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	}

	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, err
	}

	// Verify connection & credentials immediately on startup
	res, err := client.Info()
	if err != nil {
		return nil, fmt.Errorf("failed to authenticate or connect to elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch connection error status: %s", res.Status())
	}

	return &ESWriter{Client: client, Index: cfg.ElasticIndex}, nil
}

func (w *ESWriter) Write(ctx context.Context, docID string, data []byte) error {
	req := esapi.IndexRequest{
		Index:      w.Index,
		DocumentID: docID,
		Body:       bytes.NewReader(data),
		Refresh:    "false",
	}

	res, err := req.Do(ctx, w.Client)
	if err != nil {
		return fmt.Errorf("failed to index document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error indexing document: %s", res.Status())
	}

	return nil
}

func (w *ESWriter) WriteBulk(ctx context.Context, data map[string][]byte) error {
	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:     w.Client,
		Index:      w.Index,
		FlushBytes: 5_000_000,
	})
	if err != nil {
		return err
	}
	defer indexer.Close(ctx)

	for k, v := range data {
		_ = indexer.Add(ctx, esutil.BulkIndexerItem{
			Action:     "index",
			DocumentID: k,
			Body:       bytes.NewReader(v),
		})
	}

	return indexer.Flush(ctx)
}
