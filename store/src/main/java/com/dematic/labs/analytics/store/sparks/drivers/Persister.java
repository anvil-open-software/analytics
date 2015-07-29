package com.dematic.labs.analytics.store.sparks.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.util.StringUtils;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.google.common.base.Strings;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getStreamingContext;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public final class Persister implements Serializable {
    private transient static final Logger LOGGER = LoggerFactory.getLogger(Persister.class);

    public static final String RAW_EVENT_LEASE_TABLE_NAME = "Raw_Event_LT";

    /**
     * Arguments:
     * <p/>
     * Kinesis Endpoint, Kinesis StreamName, DynamoDB Endpoint, DynamoDB Table Prefix, and PollTime
     * <p/>
     * or
     * <p/>
     * Kinesis Endpoint, Kinesis StreamName, DynamoDB Endpoint, and PollTime
     */
    public static void main(final String[] args) {
        if (args.length < 4) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint, Kinesis StreamName, DynamoDB Endpoint,"
                    + "optional DynamoDB Prefix, and driver PollTime");
        }
        // url and stream name to pull events
        final String kinesisEndpoint = args[0];
        final String streamName = args[1];
        final String dynamoDBEndpoint = args[2];

        final String dynamoPrefix;
        final Duration pollTime;
        if (args.length == 4) {
            dynamoPrefix = null;
            pollTime = Durations.seconds(Integer.valueOf(args[3]));
        } else {
            dynamoPrefix = args[3];
            pollTime = Durations.seconds(Integer.valueOf(args[4]));
        }

        final String appName = Strings.isNullOrEmpty(dynamoPrefix) ? RAW_EVENT_LEASE_TABLE_NAME :
                String.format("%s%s", dynamoPrefix, RAW_EVENT_LEASE_TABLE_NAME);

        // create the table, if it does not exist
        createDynamoTable(dynamoDBEndpoint, Event.class, dynamoPrefix);

        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext = getStreamingContext(null, appName, null, pollTime);
        // persist events
        final Persister persister = new Persister();
        persister.persistEvents(getJavaDStream(kinesisEndpoint, streamName, pollTime, streamingContext),
                dynamoDBEndpoint, dynamoPrefix);
        // Start the streaming context and await termination
        LOGGER.info("starting Persister Driver with master URL >{}<", streamingContext.sc().master());
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void persistEvents(final JavaDStream<byte[]> inputStream, final String dynamoDBEndpoint, final String tablePrefix) {
        // transform the byte[] (byte arrays are json) to a string to events
        final JavaDStream<Event> eventStream =
                inputStream.map(
                        event -> EventUtils.jsonToEvent(new String(event, Charset.defaultCharset()))
                );
        //todo: handle dynamo failures and driver failures...
        // save by partition
        eventStream.foreachRDD(rdd -> {
            rdd.foreachPartition(eventIterator -> {
                final long start = System.currentTimeMillis();

                final AmazonDynamoDBClient amazonDynamoDBClient = Connections.getAmazonDynamoDBClient(dynamoDBEndpoint);
                final DynamoDBMapper dynamoDBMapper = StringUtils.isNullOrEmpty(tablePrefix) ?
                        new DynamoDBMapper(amazonDynamoDBClient) :
                        new DynamoDBMapper(amazonDynamoDBClient, new DynamoDBMapperConfig(withTableNamePrefix(tablePrefix)));
                // turn the eventIterator to a list of events and batch save
                final List<Event> collect = stream(spliteratorUnknownSize(eventIterator, Spliterator.CONCURRENT), true)
                        .collect(Collectors.<Event>toList());
                final int events = collect.size();
                final long bs = System.currentTimeMillis();
                final List<DynamoDBMapper.FailedBatch> failedBatches = dynamoDBMapper.batchSave(collect);
                final long be = System.currentTimeMillis() - bs;
                LOGGER.info("batch save time {} ms ", be);
                // todo: figure out correct solution
                if (failedBatches != null && failedBatches.size() > 0) {
                    for (final DynamoDBMapper.FailedBatch failedBatch : failedBatches) {
                        LOGGER.error("failed to save to dynamo because " + failedBatch.getException() + " " + failedBatch.getUnprocessedItems() + " " + failedBatch.getUnprocessedItems().values().size());
                    }
                    throw new IllegalArgumentException("Unable to process all batch items " + failedBatches);
                }
                final long total = System.currentTimeMillis() - start;
                LOGGER.info("total {} events in {} ms ", events, total);
            });
            return null;
        });
    }

    //todo: implement
    private void handleFailedDynamoBatchWrites(final List<DynamoDBMapper.FailedBatch> failedBatches) {
        failedBatches.stream().forEach(failedBatch -> {
            final Map<String, List<WriteRequest>> unprocessedItems = failedBatch.getUnprocessedItems();
        });
    }
}