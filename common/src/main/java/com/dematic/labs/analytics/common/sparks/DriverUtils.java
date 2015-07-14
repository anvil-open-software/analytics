package com.dematic.labs.analytics.common.sparks;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.dematic.labs.toolkit.aws.Connections;
import com.google.common.base.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kinesis.KinesisUtils;

import java.util.ArrayList;
import java.util.List;


//todo: nullable/non-nullable
public final class DriverUtils {
    private DriverUtils() {
    }

    public static int getNumberOfShards(final String awsEndpointUrl, final String streamName) {
        final AmazonKinesisClient amazonKinesisClient = Connections.getAmazonKinesisClient(awsEndpointUrl);
        // Determine the number of shards from the stream and create 1 Kinesis Worker/Receiver/DStream for each shard
        return amazonKinesisClient.describeStream(streamName).getStreamDescription().getShards().size();
    }

    public static JavaStreamingContext getStreamingContext(final String awsEndpointUrl, final String applicationName,
                                                           final String checkPointDir, final String streamName,
                                                           final Duration pollTime) {
        final JavaStreamingContextFactory factory = () -> {
            // Must add 1 more thread than the number of receivers or the output won't show properly from the driver
            final int numSparkThreads = getNumberOfShards(awsEndpointUrl, streamName) + 1;
            // Spark config
            final SparkConf configuration = new SparkConf().
                    // sets the lease manager table name
                            setAppName(applicationName).setMaster("local[" + numSparkThreads + "]");
            final JavaStreamingContext streamingContext = new JavaStreamingContext(configuration, pollTime);
            streamingContext.checkpoint(checkPointDir);
            return streamingContext;
        };
        return Strings.isNullOrEmpty(checkPointDir) ? factory.create() :
                JavaStreamingContext.getOrCreate(checkPointDir, factory);
    }

    public static JavaDStream<byte[]> getJavaDStream(final String awsEndpointUrl, final String streamName,
                                                     final Duration pollTime, final JavaStreamingContext streamingContext) {
        final int shards = getNumberOfShards(awsEndpointUrl, streamName);
        // create 1 Kinesis Worker/Receiver/DStream for each shard
        final List<JavaDStream<byte[]>> streamsList = new ArrayList<>(shards);
        for (int i = 0; i < shards; i++) {
            streamsList.add(
                    KinesisUtils.createStream(streamingContext, streamName, awsEndpointUrl, pollTime,
                            InitialPositionInStream.LATEST, StorageLevel.MEMORY_ONLY())
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
