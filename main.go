package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	cfg "pipes/config"
	"pipes/elasticsearch"
	"pipes/pipes"
	"pipes/rabbitmq"
	"syscall"

	"go.opentelemetry.io/otel/trace/noop"
)

func main() {
	loger := log.New(os.Stdout, "[pipes] ", log.Ldate)

	loger.Println("Starting Event Daemon...")

	conf := cfg.LoadConfig()

	// Initialize Elasticsearch Writer
	esWriter, err := elasticsearch.NewESWriter(conf)
	if err != nil {
		loger.Fatalf("Critical: Could not connect to Elasticsearch: %v", err)
	}
	loger.Println("Successfully connected to Elasticsearch.")

	db, err := sql.Open(conf.DatabaseDriver, conf.DatabaseURL)
	if err != nil {
		loger.Fatalf("Critical: Could not connect to database: %v", err)
	}
	loger.Println("Successfully connected to Database.")

	// Initialize RabbitMQ Consumer
	consumer, err := rabbitmq.NewConsumer(conf.RabbitMQURL, conf.RabbitMQQueue)
	if err != nil {
		loger.Fatalf("Critical: Could not connect to RabbitMQ: %v", err)
	}
	defer consumer.Close()
	loger.Println("Successfully connected to RabbitMQ.")

	messages, err := consumer.StartConsuming()
	if err != nil {
		loger.Fatalf("Critical: Failed to register consumer: %v", err)
	}

	// Setup Graceful Shutdown context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	loger.Printf("Pipeline routing running. Listening to queue: %s", conf.RabbitMQQueue)
	tracer := noop.NewTracerProvider().Tracer("moneta")

	go func() {

		// build the pipeline
		ppl := pipes.
			NewPipeline(ctx, tracer, loger).
			AddTransformer(&pipes.Validator{}).
			AddTransformer(&pipes.Modifier{}).
			AddTransformer(pipes.NewSQLWriter(db)).
			AddTransformer(pipes.NewESWriter(esWriter))

		for d := range messages {

			// execute the pipeline
			pipeError := ppl.Execute(d.Body)
			if pipeError != nil {
				log.Printf("Error transforming message: %v. Message: %s", pipeError, string(d.Body))

				// requeue
				//_ = d.Nack(false, true)

				// OR save message + error somewhere to be processed later
				_ = d.Ack(false)

				// save message and error here

				continue
			}

			loger.Printf(".")

			// remove from queue
			_ = d.Ack(false)
		}

	}()

	// Block until a signal is caught
	<-sigChan
	loger.Println("Shutdown signal received. Cleaning up resources...")
}
