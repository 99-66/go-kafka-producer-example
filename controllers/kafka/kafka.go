package kafka

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/caarlos0/env"
)

type Kafka struct {
	Topic string `env:"TOPIC"`
	Brokers []string `env:"BROKERS" envSeparator:","`
}

func (k *Kafka) NewAsyncProducer() (sarama.AsyncProducer, error){
	conf := sarama.NewConfig()
	producer, err := sarama.NewAsyncProducer(k.Brokers, conf)
	if err != nil {
		return nil, err
	}
	return producer, nil
}

func InitKafka() (*Kafka, error){
	conf := Kafka{}
	if err := env.Parse(&conf); err != nil {
		return nil, errors.New("cloud not load kafka environment variables")
	}

	return &conf, nil
}