package com.dematic.labs.analytics.common.spark;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.base.Strings;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class StreamFunctions implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamFunctions.class);

    private StreamFunctions() {
    }

    // create kafka dstream function
    private static final class CreateKafkaDStream implements Function0<JavaDStream<byte[]>> {
        private final StreamConfig streamConfig;
        private final JavaStreamingContext streamingContext;

        CreateKafkaDStream(final StreamConfig streamConfig, final JavaStreamingContext streamingContext) {
            this.streamConfig = streamConfig;
            this.streamingContext = streamingContext;
        }

        @Override
        public JavaDStream<byte[]> call() throws Exception {
            // just return a direct stream from checkpoint and
            if (!OffsetManager.manageOffsets()) {
                final JavaPairInputDStream<String, byte[]> directStream =
                        KafkaUtils.createDirectStream(streamingContext,
                                String.class,
                                byte[].class,
                                StringDecoder.class,
                                DefaultDecoder.class,
                                streamConfig.getAdditionalConfiguration(), streamConfig.getTopics());

                if (OffsetManager.logOffsets()) {
                    directStream.foreachRDD(rdd -> {
                        logOffsets(((HasOffsetRanges) rdd.rdd()).offsetRanges());
                    });
                }

                return directStream.map((Function<Tuple2<String, byte[]>, byte[]>) Tuple2::_2);
            } else {
                // manually manage and load the offsets
                final JavaInputDStream<byte[]> directStream = KafkaUtils.createDirectStream(
                        streamingContext,
                        String.class,
                        byte[].class,
                        StringDecoder.class,
                        DefaultDecoder.class,
                        byte[].class,
                        streamConfig.getAdditionalConfiguration(),
                        // load the offsets
                        readTopicOffsets(streamConfig.getTopics(),
                                CassandraConnector.apply(streamingContext.sparkContext().getConf())),
                        (Function<MessageAndMetadata<String, byte[]>, byte[]>) MessageAndMetadata::message);

                if (OffsetManager.logOffsets()) {
                    directStream.foreachRDD(rdd -> {
                        logOffsets(((HasOffsetRanges) rdd.rdd()).offsetRanges());
                    });
                }

                return directStream;
            }
        }

        private static Map<TopicAndPartition, Long> readTopicOffsets(final Set<String> topics,
                                                                     final CassandraConnector connector) {
            final Map<TopicAndPartition, Long> topicMap = new HashMap<>();
            // map each topic to its partitions
            topics.stream().forEach(topic -> {
                // 1) see if it exist in cassandra
                final OffsetRange[] offsetRanges = OffsetManager.loadOffsetRanges(topic, connector);
                if (offsetRanges == null || offsetRanges.length == 0) {
                    topicMap.put(new TopicAndPartition(topic, 0), 0L);
                } else {
                    for (final OffsetRange offsetRange : offsetRanges) {
                        topicMap.put(offsetRange.topicAndPartition(), offsetRange.fromOffset());
                    }
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
                    new CreateKafkaDStream(driverConfig.getStreamConfig(), streamingContext).call();
            // work on the streams
            streamProcessor.call(dStream);
            // set the checkpoint dir
            streamingContext.checkpoint(driverConfig.getCheckPointDir());
            // return the streaming context
            return streamingContext;
        }
    }
}
