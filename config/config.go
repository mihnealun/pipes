package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	RabbitMQURL      string
	RabbitMQQueue    string
	ElasticsearchURL string
	ElasticIndex     string
	ElasticUsername  string
	ElasticPassword  string
	ElasticAPIKey    string
	DatabaseURL      string
	DatabaseDriver   string
}

func LoadConfig() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, reading from system environment variables")
	}

	return &Config{
		RabbitMQURL:      getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		RabbitMQQueue:    getEnv("RABBITMQ_QUEUE", "openstack-events"),
		ElasticsearchURL: getEnv("ELASTICSEARCH_URL", "http://localhost:9200"),
		ElasticIndex:     getEnv("ELASTICSEARCH_INDEX", "openstack-events-logs"),
		ElasticUsername:  getEnv("ELASTICSEARCH_USERNAME", ""),
		ElasticPassword:  getEnv("ELASTICSEARCH_PASSWORD", ""),
		ElasticAPIKey:    getEnv("ELASTICSEARCH_API_KEY", ""),
		DatabaseURL:      getEnv("DATABASE_URL", ""),
		DatabaseDriver:   getEnv("DB_DRIVER", "mysql"),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
