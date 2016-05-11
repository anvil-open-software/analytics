package com.dematic.labs.analytics.ingestion.spark.drivers.event.stateless;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.util.StringUtils;
import com.dematic.labs.analytics.ingestion.spark.tables.event.EventAggregator;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Strings;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.common.spark.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.spark.DriverUtils.getStreamingContext;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;

public final class EventStreamAggregator implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamAggregator.class);

    public static final String EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME = EventAggregator.TABLE_NAME + "_LT";

    // functions
    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

    public static void main(final String[] args) {
        if (args.length < 5) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint, Kinesis StreamName, DynamoDB Endpoint,"
                    + "optional DynamoDB Prefix, driver PollTime, and aggregation by time {MINUTES,DAYS}");
        }
        // url and stream name to pull events
        final String kinesisEndpoint = args[0];
        final String streamName = args[1];
        final String dynamoDBEndpoint = args[2];

        final String dynamoPrefix;
        final Duration pollTime;
        final TimeUnit timeUnit;
        if (args.length == 5) {
            dynamoPrefix = null;
            pollTime = Durations.seconds(Integer.valueOf(args[3]));
            timeUnit = TimeUnit.valueOf(args[4]);
        } else {
            dynamoPrefix = args[3];
            pollTime = Durations.seconds(Integer.valueOf(args[4]));
            timeUnit = TimeUnit.valueOf(args[5]);
        }

        final String appName = Strings.isNullOrEmpty(dynamoPrefix) ? EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME :
                String.format("%s%s", dynamoPrefix, EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME);

        // create the table, if it does not exist
        createDynamoTable(dynamoDBEndpoint, EventAggregator.class, dynamoPrefix);
        //todo: master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext = getStreamingContext(null, appName, null, pollTime);

        // Start the streaming context and await termination
        LOGGER.info("starting Event Aggregator Driver with master URL >{}<", streamingContext.sparkContext().master());
        final EventStreamAggregator eventStreamAggregator = new EventStreamAggregator();
        eventStreamAggregator.aggregateEvents(getJavaDStream(kinesisEndpoint, streamName, streamingContext),
                dynamoDBEndpoint, dynamoPrefix, timeUnit);

        streamingContext.start();
        LOGGER.info("spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }

    public void aggregateEvents(final JavaDStream<byte[]> byteStream, final String dynamoDBEndpoint,
                                final String tablePrefix, final TimeUnit timeUnit) {

        // transform the byte[] (byte arrays are json) to a string to events, and ensure distinct within stream
        final JavaDStream<Event> eventStream =
                byteStream.map(
                        event -> jsonToEvent(new String(event, Charset.defaultCharset()))
                ).transform((Function<JavaRDD<Event>, JavaRDD<Event>>) JavaRDD::distinct);

        // map to pairs and aggregate by key
        final JavaPairDStream<String, Long> aggregates = eventStream.mapToPair(event -> {
            // Downsampling: where we reduce the event’s ISO 8601 timestamp down to timeUnit precision,
            // so for instance “2015-06-05T12:54:43.064528” becomes “2015-06-05T12:54:00.000000” for minute.
            // This downsampling gives us a fast way of bucketing or aggregating events via this downsampled key
            return Tuple2.apply(event.aggregateBy(timeUnit), 1L);
        }).reduceByKey(SUM_REDUCER);

        // save counts
        aggregates.foreachRDD(rdd -> {
            final AmazonDynamoDBClient amazonDynamoDBClient = Connections.getAmazonDynamoDBClient(dynamoDBEndpoint);
            final DynamoDBMapper dynamoDBMapper = StringUtils.isNullOrEmpty(tablePrefix) ?
                    new DynamoDBMapper(amazonDynamoDBClient) :
                    new DynamoDBMapper(amazonDynamoDBClient, new DynamoDBMapperConfig(withTableNamePrefix(tablePrefix)));
            final List<Tuple2<String, Long>> collect = rdd.collect();
            for (final Tuple2<String, Long> bucket : collect) {
                AggregationDriverUtils.createOrUpdateDynamoDBBucket(bucket, dynamoDBMapper);
            }
            return null;
        });
    }
}