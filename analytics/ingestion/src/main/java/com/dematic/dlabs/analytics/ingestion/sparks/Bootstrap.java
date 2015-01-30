package com.dematic.dlabs.analytics.ingestion.sparks;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.google.common.base.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("UnusedDeclaration")
public final class Bootstrap {
    private static final String KINESIS_ENDPOINT = "kinesisEndpoint";
    private static final String KINESIS_REGION = "regionName";
    private static final String KINESIS_STREAM = "kinesisInputStream";
    private static final String SPARKS_APP_NAME = "EventProcessor";

    private Bootstrap() {
    }

    public static AWSCredentialsProvider getAWSCredentialsProvider() {
        // AWS credentials are set in system properties via junit.properties
        return new DefaultAWSCredentialsProviderChain();
    }

    public static AmazonKinesisClient getAmazonKinesisClient() {
        // properties come from system properties in junit.properties
        final AmazonKinesisClient client = new AmazonKinesisClient(getAWSCredentialsProvider());
        client.setEndpoint(assetPropertyExist(KINESIS_ENDPOINT));
        client.setRegion(Region.getRegion(Regions.fromName(KINESIS_REGION)));
        return client;
    }

    public static SparkConf getConfiguration() {
        // run locally w 2 thread
        return new SparkConf().setMaster("local[2]").setAppName(SPARKS_APP_NAME);
    }

    public static JavaStreamingContext getStreamingContext(final SparkConf configuration) {
        return new JavaStreamingContext(configuration, Durations.seconds(2));
    }

    public static JavaDStream<byte[]> getEventStreamReceiver(final JavaStreamingContext streamingContext,
                                                                          final AmazonKinesisClient amazonKinesisClient) {
        // todo: have to configure a different stream for spark's processing, that is, kinesis would have once stream to
        // todo: to save to S3 for longer term analytics and another stream for near real-time processing with spark's
        final DescribeStreamResult describeStreamResult =
                amazonKinesisClient.describeStream(assetPropertyExist(KINESIS_STREAM));
        if (describeStreamResult == null ||
                Strings.isNullOrEmpty(describeStreamResult.getStreamDescription().getStreamName())) {
            throw new IllegalArgumentException("'{}' Kinesis stream does not exist");
        }
        final StreamDescription streamDescription = describeStreamResult.getStreamDescription();
        // determine the number of shards from the stream
        final int numShards = streamDescription.getShards().size();
        // number of spark's streams to handle all the kinesis shards
        final List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) {
            streamsList.add(KinesisUtils.createStream(streamingContext, KINESIS_STREAM, KINESIS_ENDPOINT,
                    Durations.seconds(2), InitialPositionInStream.TRIM_HORIZON, StorageLevel.MEMORY_ONLY()));
        }
        // union all the streams if there is more than 1 stream
        final JavaDStream<byte[]> unionStreams;
        if (streamsList.size() > 1) {
            unionStreams = streamingContext.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
        } else {
                        /* Otherwise, just use the 1 stream */
            unionStreams = streamsList.get(0);
        }
        return unionStreams;
    }

    private static String assetPropertyExist(final String propertyName) {
        if (Strings.isNullOrEmpty(propertyName) || Strings.isNullOrEmpty(System.getProperty(propertyName))) {
            throw new IllegalArgumentException("'{}' property does not exist");
        }
        return System.getProperty(propertyName);
    }
}
