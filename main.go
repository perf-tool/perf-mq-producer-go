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

package main

import (
	"github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"perf-mq-producer-go/conf"
	"perf-mq-producer-go/kafka"
	"perf-mq-producer-go/pulsar"
)

func main() {
	logrus.Info("performance producer start")
	switch conf.ProduceType {
	case conf.ProduceTypePulsar:
		err := pulsar.Start()
		if err != nil {
			panic(err)
		}
	case conf.ProduceTypeKafka:
		kafka.Start()
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	for {
		<-interrupt
	}
}
