// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	"perf-mq-producer-go/conf"
	"perf-mq-producer-go/util"
	"time"
)

type iProducer interface {
	initial(ctx context.Context)
	send(ctx context.Context, topic string, message []byte) error
	close()
}

var _ iProducer = (*kafkaGo)(nil)
var _ iProducer = (*kafkaSarama)(nil)
var _ iProducer = (*kafkaConfluent)(nil)

type kafkaGo struct {
	writer *kafka.Writer
}

func (kg *kafkaGo) initial(ctx context.Context) {
	transport := kafka.DefaultTransport
	if conf.KafkaSaslEnable {
		transport = &kafka.Transport{
			DialTimeout: 3 * time.Second,
			IdleTimeout: 60 * time.Second,
			ClientID:    "pf-mq",
			SASL: plain.Mechanism{
				Username: conf.KafkaSaslUsername,
				Password: conf.KafkaSaslPassword,
			},
			Context: ctx,
		}
	}
	kg.writer = &kafka.Writer{
		Addr:                   kafka.TCP(fmt.Sprintf("%s:%d", conf.KafkaHost, conf.KafkaPort)),
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		Transport:              transport,
		AllowAutoTopicCreation: conf.KafkaAutoTopicCreation,
	}
}

func (kg *kafkaGo) send(ctx context.Context, topic string, message []byte) error {
	var msg = kafka.Message{
		Topic: topic,
		Value: message,
	}
	if err := kg.writer.WriteMessages(ctx, msg); err != nil {
		return err
	}
	return nil
}

func (kg *kafkaGo) close() {
	kg.writer.Close()
}

type kafkaSarama struct {
	writer sarama.SyncProducer
}

func (ks *kafkaSarama) initial(ctx context.Context) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	if conf.KafkaSaslEnable {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.User = conf.KafkaSaslUsername
		config.Net.SASL.Password = conf.KafkaSaslPassword
	}
	producer, err := sarama.NewSyncProducer([]string{fmt.Sprintf("%s:%d",
		conf.KafkaHost, conf.KafkaPort)}, config)
	if err != nil {
		logrus.Fatalf("init producer failed: %+v", err)
	}
	ks.writer = producer
}

func (ks *kafkaSarama) send(ctx context.Context, topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	_, _, err := ks.writer.SendMessage(msg)
	if err != nil {
		return err
	}
	return nil
}

func (ks *kafkaSarama) close() {
	ks.writer.Close()
}

type kafkaConfluent struct {
	writer *confluent.Producer
}

func (kc *kafkaConfluent) initial(ctx context.Context) {
	// configmap: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	var configmap = &confluent.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%d", conf.KafkaHost, conf.KafkaPort),
		"client.id":         "pf-mq",
	}
	if conf.KafkaGroupID != "" {
		(*configmap)["group.id"] = conf.KafkaGroupID
	}
	if conf.KafkaSaslEnable {
		(*configmap)["sasl.mechanisms"] = "PLAIN"
		(*configmap)["security.protocol"] = "SASL_PLAINTEXT"
		(*configmap)["sasl.username"] = conf.KafkaSaslUsername
		(*configmap)["sasl.password"] = conf.KafkaSaslPassword
	}
	p, err := confluent.NewProducer(configmap)
	if err != nil {
		logrus.Fatalf("init producer failed: %+v", err)
	}
	kc.writer = p
}

func (kc *kafkaConfluent) send(ctx context.Context, topic string, message []byte) error {
	if err := kc.writer.Produce(&confluent.Message{
		TopicPartition: confluent.TopicPartition{Topic: &topic, Partition: confluent.PartitionAny},
		Value:          message,
	}, nil); err != nil {
		return err
	}
	return nil
}

func (kc *kafkaConfluent) close() {
	kc.writer.Close()
}

func Start() {
	for i := 0; i < conf.RoutineNum; i++ {
		go startProducer()
	}
}

func startProducer() {
	startAt := time.Now()
	rateLimit := rate.NewLimiter(rate.Limit(conf.ProduceRate), 1000)
	var dialCtx = context.Background()

	var producer iProducer
	switch conf.KafkaClientType {
	case conf.KafkaClientGo:
		producer = &kafkaGo{}
	case conf.KafkaClientSarama:
		producer = &kafkaSarama{}
	case conf.KafkaClientConfluent:
		producer = &kafkaConfluent{}
	default:
		logrus.Errorf("unsupport kafka client")
		return
	}
	producer.initial(dialCtx)
	defer producer.close()

	for {
		if conf.ProduceMinute < int(time.Since(startAt).Minutes()) {
			// always sleep
			logrus.Infof("task done")
			time.Sleep(time.Minute * 9999)
			continue
		}
		if !rateLimit.Allow() {
			continue
		}

		var data = util.RandByte(conf.KafkaMessageSize)
		if err := producer.send(dialCtx, conf.KafkaTopic, data); err != nil {
			logrus.Errorf("send message failed: %+v", err)
			return
		}

		if conf.ProduceInterval != 0 {
			time.Sleep(time.Millisecond * time.Duration(conf.ProduceInterval))
		}
	}

}
