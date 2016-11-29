package com.dematic.labs.analytics.common.spark;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.base.Strings;
import jersey.repackaged.com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.dematic.labs.analytics.common.spark.OffsetManager.initialOffsets;
import static com.dematic.labs.analytics.common.spark.OffsetManager.manageOffsets;
import static org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream;

public final class StreamFunctions implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamFunctions.class);

    private StreamFunctions() {
    }

    // create kafka dstream function
    private static final class CreateKafkaDStream implements Function0<JavaInputDStream<ConsumerRecord<String, byte[]>>> {
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
        public JavaInputDStream<ConsumerRecord<String, byte[]>> call() throws Exception {
            LOGGER.info("OFFSETS: manually manage >{}<", manageOffsets());

            final Map<TopicPartition, Long> topicPartitionsOffsets = manageOffsets() ?
                    // manually manage and load the offsets
                    readTopicPartitionOffsets(keyspace, streamConfig.getStreamEndpoint(), streamConfig.getTopics(),
                            streamingContext.sparkContext().getConf()) :
                    assignTopicPartitionOffsets(streamConfig.getStreamEndpoint(), streamConfig.getTopics());

            final JavaInputDStream<ConsumerRecord<String, byte[]>> inputDStream =
                    create(streamingContext, streamConfig, topicPartitionsOffsets);

            if (OffsetManager.logOffsets()) {
                inputDStream.foreachRDD(rdd -> logOffsets(((HasOffsetRanges) rdd.rdd()).offsetRanges()));
            }

            return inputDStream;
        }

        private static JavaInputDStream<ConsumerRecord<String, byte[]>> create(final JavaStreamingContext streamingContext,
                                                                               final StreamConfig streamConfig,
                                                                               final Map<TopicPartition, Long> tpOffset) {
            // manually assign a CS
            final ConsumerStrategy<String, byte[]> assignCS =
                    ConsumerStrategies.Assign(Lists.newArrayList(tpOffset.keySet()),
                            streamConfig.getAdditionalConfiguration(), tpOffset);
            // create the stream
            return createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), assignCS);
        }

        private static Map<TopicPartition, Long> readTopicPartitionOffsets(final String keyspace,
                                                                           final String streamEndpoint,
                                                                           final Set<String> topics,
                                                                           final SparkConf sparkConf) {
            final CassandraConnector cassandraConnector = CassandraConnector.apply(sparkConf);
            final Map<TopicPartition, Long> topicMap = new HashMap<>();
            // map each topic to its partitions
            topics.forEach(topic -> {
                // 1) see if it exist in cassandra, if not, find out # of topics and partitions from kafka
                final OffsetRange[] offsetRanges = OffsetManager.loadOffsetRanges(keyspace, topic, cassandraConnector);
                if (offsetRanges.length == 0) {
                    final List<TopicPartition> partitionList = initialOffsets(streamEndpoint, topic);
                    partitionList.forEach(tAndP -> topicMap.put(tAndP, 0L));
                } else {
                    for (final OffsetRange offsetRange : offsetRanges) {
                        topicMap.put(offsetRange.topicPartition(), offsetRange.fromOffset());
                    }
                }
            });
            return topicMap;
        }

        private static void logOffsets(final OffsetRange[] offsets) {
            for (final OffsetRange offset : offsets) {
                LOGGER.info("OFFSET: " + offset.topic() + ' ' + offset.partition() + ' ' + offset.fromOffset() + ' '
                        + offset.untilOffset());
            }
        }

        private static Map<TopicPartition, Long> assignTopicPartitionOffsets(final String streamEndpoint,
                                                                             final Set<String> topics) {
            final Map<TopicPartition, Long> topicMap = new HashMap<>();
            topics.forEach(topic -> {
                final List<TopicPartition> partitionList = initialOffsets(streamEndpoint, topic);
                // for now assign to 0, will have to look into other strategies
                partitionList.forEach(tAndP -> topicMap.put(tAndP, 0L));
            });
            return topicMap;
        }
    }

    public static final class CreateKafkaCassandraStreamingContext implements Function0<JavaStreamingContext> {
        private final CassandraDriverConfig driverConfig;
        private final VoidFunction<JavaInputDStream<ConsumerRecord<String, byte[]>>> streamProcessor;

        public CreateKafkaCassandraStreamingContext(final CassandraDriverConfig driverConfig,
                                                    final VoidFunction<JavaInputDStream<ConsumerRecord<String, byte[]>>> streamProcessor) {
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
            final JavaInputDStream<ConsumerRecord<String, byte[]>> dStream =
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
