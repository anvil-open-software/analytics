package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.dematic.labs.analytics.common.AWSConnections;
import com.dematic.labs.analytics.common.Event;
import com.dematic.labs.analytics.common.EventUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;

import static com.dematic.labs.analytics.ingestion.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.ingestion.sparks.DriverUtils.getStreamingContext;

public final class EventConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumer.class);

    public static void main(final String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException(String.format("Driver requires Kinesis Endpoint and Kinesis StreamName and DynamoDB Endpoint"));
        }
        // url and stream name to pull events
        final String kinesisEndpoint = args[0];
        final String streamName = args[1];
        final String dynamoDBEndpoint = args[2];

        // create the table, if it does not exist
        AWSConnections.createDynamoTable(kinesisEndpoint, Event.class);

        final Duration pollTime = Durations.seconds(2);
        // make Duration configurable
        final JavaStreamingContext streamingContext = getStreamingContext(kinesisEndpoint, streamName, pollTime);

        // consume events
        final EventConsumer consumer = new EventConsumer();
        consumer.persistEvents(getJavaDStream(kinesisEndpoint, streamName, pollTime, streamingContext), dynamoDBEndpoint);
        // Start the streaming context and await termination
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void persistEvents(final JavaDStream<byte[]> inputStream, final String dynamoDBEndpoint) {
        final DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(AWSConnections.getAmazonDynamoDBClient(dynamoDBEndpoint));
        // transform the byte[] (byte arrays are json) to a string
        final JavaDStream<String> eventMap =
                inputStream.map(
                        event -> new String(event, Charset.defaultCharset())
                );
        // for each distributed data set, send to an output stream, and write to a dynamo table
        eventMap.foreachRDD(
                // rdd = distributed data set
                rdd -> {
                    if (rdd.count() > 0) {
                        // todo: batch save
                        final List<String> events = rdd.collect();
                        for (final String event : events) {
                            dynamoDBMapper.save(EventUtils.jsonToEvent(event));
                        }

                    }
                    return null;
                }
        );
    }
}
