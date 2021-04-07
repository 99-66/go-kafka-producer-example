package main

import (
	"encoding/json"
	"github.com/99-66/go-kafka-producer/controllers/kafka"
	"github.com/99-66/go-kafka-producer/models"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

type kafkaConfig struct {

}

func main() {
	kafkaConf, err := kafka.InitKafka()
	if err != nil {
		panic(err)
	}

	p, err := kafkaConf.NewAsyncProducer()
	if err != nil {
		panic(err)
	}
	defer p.Close()

	ch := make(chan models.Item)
	go generateSampleItem(ch)

	for val := range ch {
		valJson, err := json.Marshal(val)
		if err != nil {
			log.Printf("item failed marchaling. %v\n", err)
			continue
		}
		msg := &sarama.ProducerMessage{
			Topic: kafkaConf.Topic,
			Value: sarama.ByteEncoder(valJson),
		}
		log.Printf("send to : %v\n", val.CreatedAt)
		p.Input() <- msg
	}
}

func generateSampleItem(ch chan<- models.Item) {
	for {
		t, _ := time.Parse(time.RFC3339, time.Now().Format(time.RFC3339))
		i := models.Item{
			Text: "Test text value.",
			Tag: "producer",
			CreatedAt: t,
		}
		ch <- i
		time.Sleep(time.Second * 1)
	}
	defer close(ch)
}