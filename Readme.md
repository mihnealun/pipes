PIPES

Simple ETL project implemented using the Chain of Responsibility pattern.

## What it does

Ingests OpenStack-style billing events, validates/transforms them, and writes them to MySQL and Elasticsearch.

Events can be ingested two ways, selected at startup via the `-mode` flag:

- `-mode=consumer` (default) — consumes messages from a RabbitMQ queue.
- `-mode=api` — starts an HTTP API (built with [Echo](https://echo.labstack.com/)) that accepts events directly.

Both modes run the same processing pipeline: `LogKeeper → Validator → Modifier → SQLWriter → ESWriter`.

## Running

```
make build              # go build -o bin/pipes .
./bin/pipes                 # consumer mode (RabbitMQ)
./bin/pipes -mode=api        # API mode (HTTP)
```

Configuration is read from a `.env` file or system environment variables — see `.env.example` for the full list. A `keeper.log` file must exist in the working directory before starting (either mode).

### API mode

```
POST /events      # ingest a raw event payload (JSON body); runs synchronously through the pipeline
GET  /healthz      # liveness check
```

`POST /events` responds `202 Accepted` on success, `422 Unprocessable Entity` if the payload is rejected by the pipeline (e.g. an unrecognized `event_type`), or `400 Bad Request` for a missing/empty body. The listen address defaults to `:8080` (override with `API_ADDR`).

## Testing

```
make tests               # unit tests
make integrationtests     # integration tests (needs Docker), covers both ingestion modes
```
