package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	cfg "pipes/config"
	"pipes/connector/ingest"
	"pipes/connector/output"
	"pipes/pipes"
	"syscall"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/trace/noop"
)

func main() {
	loger := log.New(os.Stdout, "[pipes] ", log.Ldate)

	loger.Println("Starting Event Daemon...")

	mode := flag.String("mode", "consumer", `ingestion mode: "consumer" (RabbitMQ) or "api" (HTTP API)`)
	flag.Parse()

	if *mode != "api" {
		*mode = "consumer"
	}

	conf := cfg.LoadConfig()

	// Initialize Elasticsearch Writer
	esWriter, err := output.NewESWriter(conf)
	if err != nil {
		loger.Fatalf("Critical: Could not connect to Elasticsearch: %v", err)
	}
	loger.Println("Successfully connected to Elasticsearch.")

	db, err := sql.Open(conf.DatabaseDriver, conf.DatabaseURL)
	if err != nil {
		loger.Fatalf("Critical: Could not connect to database: %v", err)
	}
	loger.Println("Successfully connected to Database.")

	// Setup Graceful Shutdown context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	tracer := noop.NewTracerProvider().Tracer("moneta")

	fi, err := os.Open("keeper.log")
	if err != nil {
		loger.Fatalf("Critical: Could not open keeper.log: %v", err)
	}

	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()

	// build the pipeline, shared by both ingestion modes
	ppl := pipes.
		NewPipeline(ctx, tracer, loger).
		AddTransformer(pipes.NewLogKeeper(fi)).
		AddTransformer(&pipes.Validator{}).
		AddTransformer(&pipes.Modifier{}).
		AddTransformer(pipes.NewSQLWriter(db)).
		AddTransformer(pipes.NewESWriter(esWriter))

	var apiConsumer *ingest.APIConsumer

	switch *mode {
	case "consumer":
		// Initialize RabbitMQ Consumer
		consumer, err := ingest.NewQueueConsumer(conf.RabbitMQURL, conf.RabbitMQQueue)
		if err != nil {
			loger.Fatalf("Critical: Could not connect to RabbitMQ: %v", err)
		}
		defer consumer.Close()
		loger.Println("Successfully connected to RabbitMQ.")

		messages, err := consumer.StartConsuming()
		if err != nil {
			loger.Fatalf("Critical: Failed to register consumer: %v", err)
		}

		loger.Printf("Pipeline routing running. Listening to queue: %s", conf.RabbitMQQueue)

		go func() {
			for d := range messages {

				go func(data amqp091.Delivery, lg *log.Logger) {
					// execute the pipeline
					pipeError := ppl.Run(data.Body)
					if pipeError != nil {
						lg.Printf("Error transforming message: %v. Message: %s", pipeError, string(data.Body))

						// requeue
						//_ = d.Nack(false, true)

						// OR save message + error somewhere to be processed later
						_ = data.Ack(false)

						// save message and error here

						return
					}

					lg.Printf(".")

					// remove from queue
					_ = data.Ack(false)
				}(d, loger)

			}
		}()

	case "api":
		apiConsumer = ingest.NewAPIConsumer(conf.APIAddr, ppl.Run)

		go func() {
			if err := apiConsumer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				loger.Fatalf("Critical: API server failed: %v", err)
			}
		}()

		loger.Printf("Pipeline routing running. Listening for API ingestion on: %s", conf.APIAddr)
	}

	// Block until a signal is caught
	<-sigChan
	loger.Println("Shutdown signal received. Cleaning up resources...")

	if apiConsumer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := apiConsumer.Shutdown(shutdownCtx); err != nil {
			loger.Printf("Error shutting down API server: %v", err)
		}
		return
	}

	time.Sleep(10 * time.Second)
}
