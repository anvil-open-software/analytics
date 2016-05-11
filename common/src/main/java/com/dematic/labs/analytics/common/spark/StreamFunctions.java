package com.dematic.labs.analytics.common.spark;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.base.Strings;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.dematic.labs.toolkit.aws.Connections.getNumberOfShards;

public final class StreamFunctions implements Serializable {
    private StreamFunctions() {
    }

    // create kinesis dstream function
    private static final class CreateKinesisDStream implements Function0<JavaDStream<byte[]>> {
        private final StreamConfig streamConfig;
        private final JavaStreamingContext streamingContext;

        CreateKinesisDStream(final StreamConfig streamConfig, final JavaStreamingContext streamingContext) {
            this.streamConfig = streamConfig;
            this.streamingContext = streamingContext;
        }

        @Override
        public JavaDStream<byte[]> call() throws Exception {
            final String kinesisEndpoint = streamConfig.getStreamEndpoint();
            final String streamName = streamConfig.getStreamName();
            // create the dstream
            final int shards = getNumberOfShards(kinesisEndpoint, streamName);
            // create 1 Kinesis Worker/Receiver/DStream for each shard
            final List<JavaDStream<byte[]>> streamsList = new ArrayList<>(shards);
            for (int i = 0; i < shards; i++) {
                streamsList.add(
                        KinesisUtils.createStream(streamingContext, streamName, kinesisEndpoint,
                                DefaultDriverConfig.getKinesisCheckpointWindow(), InitialPositionInStream.TRIM_HORIZON,
                                StorageLevel.MEMORY_ONLY())
                );
            }
            // Union all the streams if there is more than 1 stream
            final JavaDStream<byte[]> unionStreams;
            if (streamsList.size() > 1) {
                unionStreams = streamingContext.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
            } else {
                unionStreams = streamsList.get(0);
            }
            return unionStreams;
        }
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
            final JavaPairInputDStream<String, byte[]> directStream =
                    KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class, StringDecoder.class,
                            DefaultDecoder.class, streamConfig.getAdditionalConfiguration(), streamConfig.getTopics());
            // get the dstream
            return directStream.map((Function<Tuple2<String, byte[]>, byte[]>) Tuple2::_2);
        }
    }

    public static final class CreateStreamingContext implements Function0<JavaStreamingContext> {
        private final DefaultDriverConfig driverConfig;
        private final VoidFunction<JavaDStream<byte[]>> streamProcessor;

        public CreateStreamingContext(final DefaultDriverConfig driverConfig,
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
            // create the streaming context
            final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConfiguration,
                    driverConfig.getPollTimeInSeconds());

            //todo: which type of stream
            // create the dstream
            final JavaDStream<byte[]> dStream =
                    new CreateKinesisDStream(driverConfig.getStreamConfig(), streamingContext).call();

            // work on the streams
            streamProcessor.call(dStream);
            // set the checkpoint dir
            streamingContext.checkpoint(driverConfig.getCheckPointDir());
            // return the streaming context
            return streamingContext;
        }
    }

    public static final class CreateCassandraStreamingContext implements Function0<JavaStreamingContext> {
        private final CassandraDriverConfig driverConfig;
        private final VoidFunction<JavaDStream<byte[]>> streamProcessor;

        public CreateCassandraStreamingContext(final CassandraDriverConfig driverConfig,
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
            // create the streaming context
            final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConfiguration,
                    driverConfig.getPollTimeInSeconds());

            //todo: create the dstream
            final JavaDStream<byte[]> dStream =
                    new CreateKinesisDStream(driverConfig.getStreamConfig(), streamingContext).call();
            // work on the streams
            streamProcessor.call(dStream);
            // set the checkpoint dir
            streamingContext.checkpoint(driverConfig.getCheckPointDir());
            // return the streaming context
            return streamingContext;
        }
    }
}
