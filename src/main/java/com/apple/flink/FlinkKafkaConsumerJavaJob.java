/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkKafkaConsumerJavaJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumerJavaJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Kafka consumer job");

        Options options = new Options();
        options.addRequiredOption("b", "broker", true, "Kafka brokers");
        options.addRequiredOption("t", "topic", true, "Kafka topic");

        CommandLineParser parser = new DefaultParser();
        try {
            LOG.info("Parsing command line");
            CommandLine commandLine = parser.parse(options, args);

            var bootstrapServer = commandLine.getOptionValue("b");
            var topic = commandLine.getOptionValue("t");

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<String> stream =
                    env.fromSource(
                                    KafkaSource.<String>builder()
                                            .setBootstrapServers(bootstrapServer)
                                            .setTopics(topic)
                                            .setGroupId("group-id")
                                            .setStartingOffsets(OffsetsInitializer.earliest())
                                            .setValueOnlyDeserializer(new SimpleStringSchema())
                                            .build(),
                                    WatermarkStrategy.noWatermarks(),
                                    "kafka-source")
                            .uid("kafka-source-uid");

            stream.print().uid("print-sink-uid").name("print-sink");

            env.execute("flink-kafka-consumer");
        } catch (ParseException e) {
            LOG.error("Unable to parse command line options: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(FlinkKafkaConsumerJavaJob.class.getCanonicalName(), options);
        }

        LOG.info("Exiting Kafka consumer job");
    }
}
