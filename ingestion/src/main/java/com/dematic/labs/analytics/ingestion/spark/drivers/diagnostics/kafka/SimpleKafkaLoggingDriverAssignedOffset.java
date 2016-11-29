package com.dematic.labs.analytics.ingestion.spark.drivers.diagnostics.kafka;

import java.util.*;

import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/*
   Simplest possible java driver that assigns starting offset
*/


public final class SimpleKafkaLoggingDriverAssignedOffset {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaLoggingDriverAssignedOffset.class);
    public static final String APP_NAME = "TEST_ASSIGNED_OFFSET";


    public static void main(final String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException("Driver passed in incorrect parameters" +
                    "Usage: SimpleKafkaLoggingDriverAssignedOffset <broker bootstrap servers> <topic> <groupId> <partitionSize> ");
        }
        String kafka_topic = args[1];
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers",args[0]);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", args[2]);
        // kafkaParams.put("auto.offset.reset",args[3]);
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(kafka_topic);
        final SparkConf sparkConfiguration = new SparkConf().setAppName(APP_NAME+"_"+args[1]);
        int totalPartitions= Integer.valueOf(args[3]);
        // create the streaming context
        final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConfiguration, Durations.seconds(Integer.valueOf(args[3])));
        // force log
        streamingContext.ssc().sc().setLogLevel("DEBUG");

        // assign fixed topic partitions starting at 0
        final Map<TopicPartition,Long> partitionStart=new HashedMap();
        for (int i=0; i<totalPartitions; i++ ) {
            partitionStart.put(new TopicPartition(kafka_topic, i), Long.valueOf(0));
        }

        Assign fixedAssignment = new Assign (partitionStart.keySet(),kafkaParams, partitionStart);
        final JavaInputDStream<ConsumerRecord<String, String>> directStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        fixedAssignment
                );


         directStream.foreachRDD(rdd -> {
             LOGGER.info("OFFSET: " + rdd.rdd().name() + " " + rdd.toString());
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
