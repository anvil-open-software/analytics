package com.dematic.labs.analytics.store.sparks.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.util.StringUtils;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.nio.charset.Charset;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.*;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getStreamingContext;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;

public final class Persister implements Serializable {
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
        createDynamoTable(dynamoDBEndpoint, Event.class, null);

        final Duration pollTime = Durations.seconds(2);
        // make Duration configurable
        final JavaStreamingContext streamingContext = getStreamingContext(kinesisEndpoint, RAW_EVENT_LEASE_TABLE_NAME,
                streamName, pollTime);

        // persist events
        final Persister persister = new Persister();
        persister.persistEvents(getJavaDStream(kinesisEndpoint, streamName, pollTime, streamingContext),
                dynamoDBEndpoint, null);
        // Start the streaming context and await termination
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void persistEvents(final JavaDStream<byte[]> inputStream, final String dynamoDBEndpoint, final String tablePrefix) {
        // transform the byte[] (byte arrays are json) to a string to events
        final JavaDStream<Event> eventStream =
                inputStream.map(
                        event -> EventUtils.jsonToEvent(new String(event, Charset.defaultCharset()))
                );
        // save by partition
        eventStream.foreachRDD(v1 -> {
            v1.foreachPartition(eventIterator -> {
                //todo look into batching the events and time to create dynamo connection and figure out exception handling
                final AmazonDynamoDBClient amazonDynamoDBClient = Connections.getAmazonDynamoDBClient(dynamoDBEndpoint);
                eventIterator.forEachRemaining(event -> {
                    final DynamoDBMapper dynamoDBMapper = StringUtils.isNullOrEmpty(tablePrefix) ?
                            new DynamoDBMapper(amazonDynamoDBClient) :
                            new DynamoDBMapper(amazonDynamoDBClient, new DynamoDBMapperConfig(withTableNamePrefix(tablePrefix)));
                    dynamoDBMapper.save(event);});
            });
            return null;
        });
    }
}