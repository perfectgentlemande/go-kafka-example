package main

import (
	"encoding/json"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/perfectgentlemande/go-kafka-example/message"
	"github.com/rs/zerolog"
)

func main() {
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot create producer")
	}
	defer producer.Close()

	msg := message.Message{
		ID:        uuid.NewString(),
		Value:     "Hello",
		CreatedAt: time.Now(),
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot marshal json")
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "ping",
		Key:   sarama.StringEncoder(msg.ID),
		Value: sarama.ByteEncoder(bytes),
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to send message to Kafkar")
	}
}
