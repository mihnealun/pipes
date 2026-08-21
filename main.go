package main

import (
	"context"
	"database/sql"
	"log"
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

	// Setup Graceful Shutdown context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	loger.Printf("Pipeline routing running. Listening to queue: %s", conf.RabbitMQQueue)
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

	go func() {

		// build the pipeline
		ppl := pipes.
			NewPipeline(ctx, tracer, loger).
			AddTransformer(pipes.NewLogKeeper(fi)).
			AddTransformer(&pipes.Validator{}).
			AddTransformer(&pipes.Modifier{}).
			AddTransformer(pipes.NewSQLWriter(db)).
			AddTransformer(pipes.NewESWriter(esWriter))

		for d := range messages {

			go func(data amqp091.Delivery, lg *log.Logger) {
				// execute the pipeline
				pipeError := ppl.Execute(data.Body)
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

	// Block until a signal is caught
	<-sigChan
	loger.Println("Shutdown signal received. Cleaning up resources...")

	time.Sleep(10 * time.Second)
}
