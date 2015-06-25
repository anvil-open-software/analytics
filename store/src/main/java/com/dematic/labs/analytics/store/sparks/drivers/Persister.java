package com.dematic.labs.analytics.store.sparks.drivers;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;

import static com.dematic.labs.analytics.common.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getStreamingContext;

public final class Persister {
    private static final Logger LOGGER = LoggerFactory.getLogger(Persister.class);

    public static final String RAW_EVENT_LEASE_TABLE_NAME = "Raw_Event_LT";

    public static void main(final String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint and Kinesis StreamName and DynamoDB Endpoint");
        }
        // url and stream name to pull events
        final String kinesisEndpoint = args[0];
        final String streamName = args[1];
        final String dynamoDBEndpoint = args[2];

        // create the table, if it does not exist
        Connections.createDynamoTable(dynamoDBEndpoint, Event.class);

        final Duration pollTime = Durations.seconds(2);
        // make Duration configurable
        final JavaStreamingContext streamingContext = getStreamingContext(kinesisEndpoint, RAW_EVENT_LEASE_TABLE_NAME,
                streamName, pollTime);

        // persist events
        final Persister persister = new Persister();
        persister.persistEvents(getJavaDStream(kinesisEndpoint, streamName, pollTime, streamingContext), dynamoDBEndpoint);
        // Start the streaming context and await termination
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void persistEvents(final JavaDStream<byte[]> inputStream, final String dynamoDBEndpoint) {
        final DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(Connections.getAmazonDynamoDBClient(dynamoDBEndpoint));
        // transform the byte[] (byte arrays are json) to a string to events
        final JavaDStream<Event> eventStream =
                inputStream.map(
                        event -> EventUtils.jsonToEvent(new String(event, Charset.defaultCharset()))
                );
        // for each distributed data set, send to an output stream, and write to a dynamo table
        eventStream.foreachRDD(
                // rdd = distributed data set
                rdd -> {
                    if (rdd.count() > 0) {
                        // todo: batch save
                        final List<Event> events = rdd.collect();
                        for (final Event event : events) {
                            dynamoDBMapper.save(event);
                            LOGGER.info("saved event >{}<", event.getEventId());
                        }
                    }
                    return null;
                }
        );
    }
}
