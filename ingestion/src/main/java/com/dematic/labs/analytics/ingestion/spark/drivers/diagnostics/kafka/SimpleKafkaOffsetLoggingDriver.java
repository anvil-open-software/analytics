/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.ingestion.spark.drivers.diagnostics.kafka;

import com.dematic.labs.analytics.common.spark.KafkaStreamConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/*
   Simplest possible java driver that logs offset
*/


public final class SimpleKafkaOffsetLoggingDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaOffsetLoggingDriver.class);
    private static final String APP_NAME = "TEST_ASSIGNED_OFFSET";


    public static void main(final String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException("Driver passed in incorrect parameters" +
                    "Usage: SimpleKafkaLoggingDriverAssignedOffset <broker bootstrap servers> <topic> <groupId> <offsetReset>");
        }

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", args[0]);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", args[2]);
        kafkaParams.put("auto.offset.reset", args[3]);
        kafkaParams.put("enable.auto.commit", false);
        // add anything else from the jvm system properties
        KafkaStreamConfig.addPrefixedSystemProperties(kafkaParams, "kafka.additionalconfig.");
        // try increasing just in case...
        kafkaParams.put("session.timeout.ms", "30000");

        Collection<String> topics = Collections.singletonList(args[1]);
        final SparkConf sparkConfiguration = new SparkConf().setAppName(APP_NAME + "_" + args[1]);

        // create the streaming context
        final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConfiguration, Durations.seconds(5));
        // force log
        streamingContext.ssc().sc().setLogLevel("ALL");

        final JavaInputDStream<ConsumerRecord<String, String>> directStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        directStream.foreachRDD(rdd -> {
            LOGGER.info("OFFSET RDD: " + rdd.rdd().name() + " " + rdd.toString());
            for (final OffsetRange offset : ((HasOffsetRanges) rdd.rdd()).offsetRanges()) {
                LOGGER.info("OFFSET: " + offset.topic() + ' ' + offset.partition() + ' ' + offset.fromOffset() + ' '
                        + offset.untilOffset());
            }
        });

        // Start the streaming context and await termination
        LOGGER.info("KCP: starting SimpleKafkaLoggingDriverAssignedOffset Driver with master URL >{}<",
                streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("KCP: spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }
}
