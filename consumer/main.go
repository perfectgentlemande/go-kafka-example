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

type Message struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

// responseChannels - словарь для хранения каналов ответов, индексированных по ID запроса
// mu - мьютекс для обеспечения синхронизации доступа к словарю responseChannels
var responseChannels map[string]chan *sarama.ConsumerMessage
var mu sync.Mutex

func main() {
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()
	signalCtx, _ := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)
	responseChannels = make(map[string]chan *sarama.ConsumerMessage)

	// Создание консьюмера Kafka
	consumer, err := sarama.NewConsumer([]string{"kafka:9092"}, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create consumer")
	}
	defer consumer.Close()

	// Подписка на партицию "pong" в Kafka
	partConsumer, err := consumer.ConsumePartition("pong", 0, sarama.OffsetNewest)
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
				log.Println("Context done, exiting goroutine")
				return
			case msg, ok := <-partConsumer.Messages():
				if !ok {
					log.Println("Channel closed, exiting goroutine")
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
