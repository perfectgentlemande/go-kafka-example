package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

func main() {
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()
	signalCtx, _ := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)

	// Создание консьюмера Kafka
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create consumer")
	}
	defer consumer.Close()

	// Подписка на партицию "pong" в Kafka
	partConsumer, err := consumer.ConsumePartition("ping", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to consume partition")
	}
	defer partConsumer.Close()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Горутина для обработки входящих сообщений от Kafka
	go func() {
		defer wg.Done()

		for {
			select {
			case <-signalCtx.Done():
				log.Info().Msg("Context done, exiting goroutine")
				return
			case msg, ok := <-partConsumer.Messages():
				if !ok {
					log.Info().Msg("Channel closed, exiting goroutine")
					return
				}

				log.Info().Fields(map[string]interface{}{
					"payload": string(msg.Value),
				}).Msg("caught message")
			}
		}
	}()

	wg.Wait()
}
