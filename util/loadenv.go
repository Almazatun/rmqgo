package util

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type EnvsConfig struct {
	User string
	Pass string
	Host string
	Port string
}

func GetENVs() *EnvsConfig {
	err := godotenv.Load("./.env")

	if os.Getenv("APP_ENV") == "test" {
		return &EnvsConfig{
			User: os.Getenv("RABBITMQ_USER"),
			Pass: os.Getenv("RABBITMQ_PASS"),
			Host: os.Getenv("RABBITMQ_HOST"),
			Port: os.Getenv("RABBITMQ_PORT"),
		}
	}

	if err != nil {
		log.Fatalln("Error loading .env variables", err)
		return nil
	}

	return nil
}
