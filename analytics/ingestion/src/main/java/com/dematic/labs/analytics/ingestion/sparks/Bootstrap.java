package com.dematic.labs.analytics.ingestion.sparks;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.google.common.base.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("UnusedDeclaration")
public final class Bootstrap {
    // for now, these properties need to be set via system properties
    // used in testing
    public static final String SPARKS_APP_NAME = "EventProcessor";

    private Bootstrap() {
    }

    public static AWSCredentialsProvider getAWSCredentialsProvider() {
        // AWS credentials are set in system properties via junit.properties
        return new DefaultAWSCredentialsProviderChain();
    }

    public static SparkConf getLocalConfiguration() {
        // run locally w 2 thread
        return new SparkConf().setMaster("local[2]").setAppName(SPARKS_APP_NAME);
    }

    public static JavaStreamingContext getStreamingContext(final SparkConf configuration, final Duration duration) {
        return new JavaStreamingContext(configuration, duration);
    }

    public static JavaDStream<byte[]> getEventStreamReceiver(final JavaStreamingContext streamingContext,
                                                             final AmazonKinesisClient amazonKinesisClient,
                                                             final KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        // todo: have to configure a different stream for spark's processing, that is, kinesis would have once stream to
        // todo: to save to S3 for longer term analytics and another stream for near real-time processing with spark's
        final String streamName = kinesisConnectorConfiguration.KINESIS_INPUT_STREAM;
        final String kinesisEndpoint = kinesisConnectorConfiguration.KINESIS_ENDPOINT;
        final DescribeStreamResult describeStreamResult =
                amazonKinesisClient.describeStream(streamName);
        if (describeStreamResult == null ||
                Strings.isNullOrEmpty(describeStreamResult.getStreamDescription().getStreamName())) {
            throw new IllegalArgumentException(String.format("'%s' Kinesis stream does not exist",
                    kinesisConnectorConfiguration.KINESIS_INPUT_STREAM));
        }
        final StreamDescription streamDescription = describeStreamResult.getStreamDescription();
        // determine the number of shards from the stream
        final int numShards = streamDescription.getShards().size();
        // number of spark's streams to handle all the kinesis shards
        final List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) {
            streamsList.add(KinesisUtils.createStream(streamingContext, streamName, kinesisEndpoint,
                    Durations.seconds(2), InitialPositionInStream.TRIM_HORIZON, StorageLevel.MEMORY_ONLY()));
        }
        // union all the streams if there is more than 1 stream
        final JavaDStream<byte[]> unionStreams;
        if (streamsList.size() > 1) {
            unionStreams = streamingContext.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
        } else {
            // Otherwise, just use the 1 stream
            unionStreams = streamsList.get(0);
        }
        return unionStreams;
    }
}
