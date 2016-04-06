package com.dematic.labs.analytics.common.spark;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.base.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.dematic.labs.analytics.common.spark.DriverUtils.getKinesisCheckpointWindow;
import static com.dematic.labs.toolkit.aws.Connections.getNumberOfShards;

public final class StreamFunctions implements Serializable {
    private StreamFunctions() {
    }

    // create stream function
    private static final class CreateDStreamFunction implements Function0<JavaDStream<byte[]>> {
        private final DriverConfig driverConfig;
        private final JavaStreamingContext streamingContext;

        CreateDStreamFunction(final DriverConfig driverConfig, final JavaStreamingContext streamingContext) {
            this.driverConfig = driverConfig;
            this.streamingContext = streamingContext;
        }

        @Override
        public JavaDStream<byte[]> call() throws Exception {
            final String kinesisEndpoint = driverConfig.getKinesisEndpoint();
            final String streamName = driverConfig.getKinesisStreamName();
            // create the dstream
            final int shards = getNumberOfShards(kinesisEndpoint, streamName);
            // create 1 Kinesis Worker/Receiver/DStream for each shard
            final List<JavaDStream<byte[]>> streamsList = new ArrayList<>(shards);
            for (int i = 0; i < shards; i++) {
                streamsList.add(
                        KinesisUtils.createStream(streamingContext, streamName, kinesisEndpoint,
                                getKinesisCheckpointWindow(), InitialPositionInStream.TRIM_HORIZON,
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

    public static final class CreateStreamingContextFunction implements Function0<JavaStreamingContext> {
        private final DriverConfig driverConfig;
        private final VoidFunction<JavaDStream<byte[]>> eventStreamProcessor;

        public CreateStreamingContextFunction(final DriverConfig driverConfig,
                                              final VoidFunction<JavaDStream<byte[]>> eventStreamProcessor) {
            this.driverConfig = driverConfig;
            this.eventStreamProcessor = eventStreamProcessor;
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
                    driverConfig.getPollTime());
            // create the dstream
            final JavaDStream<byte[]> dStream =
                    new CreateDStreamFunction(driverConfig, streamingContext).call();
            // work on the streams
            eventStreamProcessor.call(dStream);
            // set the checkpoint dir
            streamingContext.checkpoint(driverConfig.getCheckPointDir());
            // return the streaming context
            return streamingContext;
        }
    }

    public static final class CreateCassandraStreamingContextFunction implements Function0<JavaStreamingContext> {
        private final CassandraDriverConfig driverConfig;
        private final VoidFunction<JavaDStream<byte[]>> eventStreamProcessor;

        public CreateCassandraStreamingContextFunction(final CassandraDriverConfig driverConfig,
                                                       final VoidFunction<JavaDStream<byte[]>> eventStreamProcessor) {
            this.driverConfig = driverConfig;
            this.eventStreamProcessor = eventStreamProcessor;
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
                    driverConfig.getPollTime());
            // create the dstream
            final JavaDStream<byte[]> dStream =
                    new CreateDStreamFunction(driverConfig, streamingContext).call();
            // work on the streams
            eventStreamProcessor.call(dStream);
            // set the checkpoint dir
            streamingContext.checkpoint(driverConfig.getCheckPointDir());
            // return the streaming context
            return streamingContext;
        }
    }
}
