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
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	"perf-mq-producer-go/conf"
	"perf-mq-producer-go/util"
	"time"
)

func Start() {
	for i := 0; i < conf.RoutineNum; i++ {
		go startProducer()
	}
}

func startProducer() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer([]string{fmt.Sprintf("%s:%d",
		conf.KafkaHost, conf.KafkaPort)}, config)
	if err != nil {
		logrus.Errorf("init producer failed: %+v", err)
		return
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: conf.KafkaTopic,
	}
	startAt := time.Now()
	rateLimit := rate.NewLimiter(rate.Limit(conf.ProduceRate), 1000)
	for {
		if conf.ProduceMinute < int(time.Since(startAt).Minutes()) {
			continue
		}
		if !rateLimit.Allow() {
			continue
		}
		msg.Value = sarama.ByteEncoder(util.RandStr(conf.KafkaMessageSize))
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			logrus.Errorf("send message failed: %v", err)
			return
		}
		if conf.ProduceInterval != 0 {
			time.Sleep(time.Millisecond * time.Duration(conf.ProduceInterval))
		}
	}
}
