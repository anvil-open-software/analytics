package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.dematic.labs.analytics.ingestion.sparks.Bootstrap;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Counts the number of events.
 */
public final class EventCount implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventCount.class);

    private EventCount() {
    }

    public static void main(final String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException(String.format("Driver requires Kinesis Endpoint and Kinesis StreamName"));
        }
        // url and stream name to pull events
        final String endpointUrl = args[0];
        final String streamName = args[1];
        // will credentials from system properties
        final AmazonKinesisClient kinesisClient = new AmazonKinesisClient(Bootstrap.getAWSCredentialsProvider());
        kinesisClient.setEndpoint(endpointUrl);


        // Determine the number of shards from the stream and create 1 Kinesis Worker/Receiver/DStream for each shard
        final int numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();
        // Must add 1 more thread than the number of receivers or the output won't show properly from the driver
        final int numSparkThreads = numShards + 1;

        // Spark config
        final SparkConf conf = new SparkConf().
                setAppName(Bootstrap.SPARKS_APP_NAME).setMaster("local[" + numSparkThreads + "]");

        final Duration pullTime = Durations.seconds(2);
        // make Duration configurable
        final JavaStreamingContext streamingContext = Bootstrap.getStreamingContext(conf, pullTime);

        // create 1 Kinesis Worker/Receiver/DStream for each shard
        final List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) {
            streamsList.add(
                    KinesisUtils.createStream(streamingContext, streamName, endpointUrl, pullTime,
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
        // count events
        final EventCount eventCount = new EventCount();
        eventCount.countEvents(unionStreams);
        // Start the streaming context and await termination
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void countEvents(final JavaDStream<byte[]> inputStream) {
        // transform the byte[] (byte arrays are json) to a string
        final JavaDStream<String> eventMap =
                inputStream.map(
                        event -> new String(event, Charset.defaultCharset())
                );
        // for each distributed data set, send to an output stream, for now we just log
        eventMap.foreachRDD(
                // rdd = distributed data set
                rdd -> {
                    if (rdd.count() > 0) {
                        final List<String> events = rdd.collect();
                        events.stream().forEach(LOGGER::info);
                        LOGGER.info("received >{}< events", events.size());
                    }
                    return null;
                }
        );
    }
}