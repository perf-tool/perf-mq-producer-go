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

package conf

import (
	"os"
	"perf-mq-producer-go/util"
)

// common config
var (
	ProduceRate     = util.GetEnvInt("PRODUCE_RATE", 1000)
	ProduceMinute   = util.GetEnvInt("PRODUCE_MINUTE", 60)
	ProduceInterval = util.GetEnvInt("PRODUCE_INTERVAL", 0)
)

// pulsar environment config
var (
	PulsarHost        = util.GetEnvStr("PULSAR_HOST", "localhost")
	PulsarPort        = util.GetEnvInt("PULSAR_PORT", 6650)
	PulsarTopic       = os.Getenv("PULSAR_TOPIC")
	PulsarMessageSize = util.GetEnvInt("PULSAR_MESSAGE_SIZE", 1024)
)

// kafka environment config
var (
	KafkaHost        = util.GetEnvStr("KAFKA_HOST", "localhost")
	KafkaPort        = util.GetEnvInt("KAFKA_PORT", 9092)
	KafkaTopic       = util.GetEnvStr("KAFKA_TOPIC", "testTopic")
	KafkaMessageSize = util.GetEnvInt("KAFKA_MESSAGE_SIZE", 1024)
)
