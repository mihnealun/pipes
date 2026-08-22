BINARY := bin/pipes

.PHONY: build tests integrationtests

build:
	go build -o $(BINARY) .

tests:
	go test ./... -count=1

integrationtests:
	go test -tags=integration ./integration/... -count=1 -timeout=10m -v
