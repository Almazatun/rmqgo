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

func GetENVs() EnvsConfig {
	err := godotenv.Load("../.env")

	if err != nil {
		log.Fatalln("Error loading .env variables", err)
	}

	return EnvsConfig{
		User: os.Getenv("RABBITMQ_USER"),
		Pass: os.Getenv("RABBITMQ_PASS"),
		Host: os.Getenv("RABBITMQ_HOST"),
		Port: os.Getenv("RABBITMQ_PORT"),
	}
}
