package com.dematic.labs.analytics.common.spark;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.base.Strings;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream;

public final class StreamFunctions implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamFunctions.class);

    private StreamFunctions() {
    }

    // create kafka dstream function
    private static final class CreateKafkaDStream implements Function0<JavaDStream<byte[]>> {
        private final String keyspace;
        private final StreamConfig streamConfig;
        private final JavaStreamingContext streamingContext;

        CreateKafkaDStream(final String keyspace, final StreamConfig streamConfig,
                           final JavaStreamingContext streamingContext) {
            this.keyspace = keyspace;
            this.streamConfig = streamConfig;
            this.streamingContext = streamingContext;
        }

        @Override
        public JavaDStream<byte[]> call() throws Exception {
            final Map<TopicPartition, Long> topicAndPartitions = OffsetManager.manageOffsets() ?
                    // manually manage and load the offsets
                    readTopicOffsets(keyspace, streamConfig.getTopics(),
                            CassandraConnector.apply(streamingContext.sparkContext().getConf())) :
                    Collections.emptyMap();
            return create(streamingContext, streamConfig, topicAndPartitions);
        }

        private static JavaDStream<byte[]> create(final JavaStreamingContext streamingContext,
                                                  final StreamConfig streamConfig,
                                                  final Map<TopicPartition, Long> topicAndPartitions) {
            // create the consumer strategy for managing offsets, of no offset is given for a TopicPartition,
            // the committed offset (if applicable) or kafka param auto.offset.reset will be used.
            final ConsumerStrategy<String, byte[]> cs =
                    ConsumerStrategies.<String, byte[]>Subscribe(streamConfig.getTopics(),
                            streamConfig.getAdditionalConfiguration(), topicAndPartitions);
            // create the stream
            final JavaInputDStream<ConsumerRecord<String, byte[]>> directStream =
                    createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), cs);
            // log offsets if needed
            if (OffsetManager.logOffsets()) {
                directStream.foreachRDD(rdd -> {
                    logOffsets(((HasOffsetRanges) rdd.rdd()).offsetRanges());
                });
            }
            // get the dstream
            return directStream.map((Function<ConsumerRecord<String, byte[]>, byte[]>) ConsumerRecord::value);
        }

        private static Map<TopicPartition, Long> readTopicOffsets(final String keyspace, final Set<String> topics,
                                                                  final CassandraConnector connector) {
            final Map<TopicPartition, Long> topicMap = new HashMap<>();
            // map each topic to its partitions
            topics.stream().forEach(topic -> {
                // 1) see if it exist in cassandra, if not just return an empty map
                final OffsetRange[] offsetRanges = OffsetManager.loadOffsetRanges(keyspace, topic, connector);
                for (final OffsetRange offsetRange : offsetRanges) {
                    topicMap.put(offsetRange.topicPartition(), offsetRange.fromOffset());
                }
            });
            return topicMap;
        }

        private static void logOffsets(final OffsetRange[] offsets) {
            for (final OffsetRange offset : offsets) {
                LOGGER.info("OFFSET: " + offset.topic() + ' ' + offset.partition() + ' ' +
                        offset.fromOffset() + ' ' + offset.untilOffset());
            }
        }
    }

    public static final class CreateKafkaCassandraStreamingContext implements Function0<JavaStreamingContext> {
        private final CassandraDriverConfig driverConfig;
        private final VoidFunction<JavaDStream<byte[]>> streamProcessor;

        public CreateKafkaCassandraStreamingContext(final CassandraDriverConfig driverConfig,
                                                    final VoidFunction<JavaDStream<byte[]>> streamProcessor) {
            this.driverConfig = driverConfig;
            this.streamProcessor = streamProcessor;
        }

        @Override
        public JavaStreamingContext call() throws Exception {
            // create spark configure
            final SparkConf sparkConfiguration = new SparkConf().setAppName(driverConfig.getAppName());
            // if master url set, apply
            if (!Strings.isNullOrEmpty(driverConfig.getMasterUrl())) {
                sparkConfiguration.setMaster(driverConfig.getMasterUrl());
            }
            // set the authorization
            sparkConfiguration.set(CassandraDriverConfig.AUTH_USERNAME_PROP, driverConfig.getUsername());
            sparkConfiguration.set(CassandraDriverConfig.AUTH_PASSWORD_PROP, driverConfig.getPassword());
            // set the connection host
            sparkConfiguration.set(CassandraDriverConfig.CONNECTION_HOST_PROP, driverConfig.getHost());
            // set the connection keep alive
            sparkConfiguration.set(CassandraDriverConfig.KEEP_ALIVE_PROP, driverConfig.getKeepAlive());
            // create the streaming context
            final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConfiguration,
                    driverConfig.getPollTimeInSeconds());

            final JavaDStream<byte[]> dStream =
                    new CreateKafkaDStream(driverConfig.getKeySpace(), driverConfig.getStreamConfig(), streamingContext)
                            .call();
            // work on the streams
            streamProcessor.call(dStream);
            // set the checkpoint dir
            streamingContext.checkpoint(driverConfig.getCheckPointDir());
            // return the streaming context
            return streamingContext;
        }
    }
}
