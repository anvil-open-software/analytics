package com.dematic.labs.analytics.common.spark;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.base.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.dematic.labs.toolkit.aws.Connections.getNumberOfShards;

/**
 * Use <class><com.dematic.labs.analytics.common.spark.StreamFunctions/class>
 */
@Deprecated
public final class DriverUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(DriverUtils.class);

    private DriverUtils() {
    }

    public static JavaStreamingContext getStreamingContext(final String masterUrl, final String applicationName,
                                                           final String checkPointDir,
                                                           final Duration pollTime) {
        final JavaStreamingContextFactory factory = () -> {
            // Spark configuration
            final SparkConf configuration = new SparkConf().
                    // sets the lease manager table name
                            setAppName(applicationName);
            if (!Strings.isNullOrEmpty(masterUrl)) {
                configuration.setMaster(masterUrl);
            }
            final JavaStreamingContext streamingContext = new JavaStreamingContext(configuration, pollTime);
            if (!Strings.isNullOrEmpty(checkPointDir)) {
                streamingContext.checkpoint(checkPointDir);
            }
            return streamingContext;
        };
        return Strings.isNullOrEmpty(checkPointDir) ? factory.create() :
                JavaStreamingContext.getOrCreate(checkPointDir, factory);
    }

    /**
     *
     * @return duration taken from environment variable spark.kinesis.checkpoint.window
     */
    public static Duration getKinesisCheckpointWindow() {
        long default_window = 30; //default 30 seconds.
        Duration window;
        String windowStr = System.getProperty(DriverConsts.SPARK_KINESIS_CHECKPOINT_WINDOW_IN_SECONDS);
        if (!Strings.isNullOrEmpty(windowStr) ) {
            window = Durations.seconds(Integer.valueOf(windowStr));
        } else {
            window= Durations.seconds(default_window);
        }
        LOGGER.info("using >{}< Kinesis checkpoiting window", window);
        return window;
    }

    public static JavaDStream<byte[]> getJavaDStream(final String awsEndpointUrl, final String streamName,
                                                     final JavaStreamingContext streamingContext) {
        final int shards = getNumberOfShards(awsEndpointUrl, streamName);
        // create 1 Kinesis Worker/Receiver/DStream for each shard
        final List<JavaDStream<byte[]>> streamsList = new ArrayList<>(shards);
        for (int i = 0; i < shards; i++) {
            streamsList.add(
                    KinesisUtils.createStream(streamingContext, streamName, awsEndpointUrl,
                            getKinesisCheckpointWindow(),
                            InitialPositionInStream.TRIM_HORIZON, StorageLevel.MEMORY_ONLY())
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
