package main

import (
	"os"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Message struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

func main() {
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create producer")
	}
	defer producer.Close()

	msg := Message{
		ID:    uuid.NewString(),
		Value: "Hello",
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "ping",
		Key:   sarama.StringEncoder(msg.ID),
		Value: sarama.StringEncoder(msg.Value),
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to send message to Kafkar")
	}
	return
}
