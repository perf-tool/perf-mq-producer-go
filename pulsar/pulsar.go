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

package pulsar

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	"perf-mq-producer-go/conf"
	"perf-mq-producer-go/util"
	"time"
)

func Start() error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: fmt.Sprintf("pulsar://%s:%d", conf.PulsarHost, conf.PulsarPort),
	})
	if err != nil {
		return err
	}
	for i := 0; i < conf.RoutineNum; i++ {
		go startProducer(client)
	}
	return nil
}

func startProducer(client pulsar.Client) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: conf.PulsarTopic,
	})
	if err != nil {
		logrus.Errorf("create producer %s error: %v", conf.PulsarTopic, err)
	}
	startAt := time.Now()
	rateLimit := rate.NewLimiter(rate.Limit(conf.ProduceRate), 1000)
	for {
		if conf.ProduceMinute < int(time.Since(startAt).Minutes()) {
			// task done, always sleep
			logrus.Infof("task done")
			time.Sleep(time.Minute * 9999)
			continue
		}
		if !rateLimit.Allow() {
			continue
		}
		messageID, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: util.RandByte(conf.PulsarMessageSize),
		})
		if err != nil {
			logrus.Errorf("send message %s error: %v", conf.PulsarTopic, err)
		}
		if conf.ProducePrintInfo {
			logrus.Infof("send message %s success, messageID: %s", conf.PulsarTopic, messageID)
		}

		if conf.ProduceInterval != 0 {
			time.Sleep(time.Millisecond * time.Duration(conf.ProduceInterval))
		}
	}
}
