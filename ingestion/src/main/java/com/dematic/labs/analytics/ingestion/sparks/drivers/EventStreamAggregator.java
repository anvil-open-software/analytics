package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.util.StringUtils;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.google.common.base.Strings;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getStreamingContext;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;

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
        if (args.length == 4) {
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
        LOGGER.info("starting Event Aggregator Driver with master URL >{}<", streamingContext.sc().master());
        EventStreamAggregator eventStreamAggregator = new EventStreamAggregator();
        eventStreamAggregator.aggregateEvents(getJavaDStream(kinesisEndpoint, streamName, pollTime, streamingContext),
                dynamoDBEndpoint, dynamoPrefix, timeUnit);

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void aggregateEvents(final JavaDStream<byte[]> inputStream, final String dynamoDBEndpoint,
                                final String tablePrefix, final TimeUnit timeUnit) {

        final JavaPairDStream<String, Long> pairs = inputStream.mapToPair(event -> {
            // Downsampling: where we reduce the event’s ISO 8601 timestamp down to timeUnit precision,
            // so for instance “2015-06-05T12:54:43.064528” becomes “2015-06-05T12:54:00.000000” for minute.
            // This downsampling gives us a fast way of bucketing or aggregating events via this downsampled key
            return new Tuple2<String, Long>(EventUtils.jsonToEvent(
                    new String(event, Charset.defaultCharset())).aggregateBy(timeUnit), 1L);
        });

        // aggregate by key
        pairs.reduceByKey(SUM_REDUCER).foreachRDD(rdd -> {
            final AmazonDynamoDBClient amazonDynamoDBClient = Connections.getAmazonDynamoDBClient(dynamoDBEndpoint);
            final DynamoDBMapper dynamoDBMapper = StringUtils.isNullOrEmpty(tablePrefix) ?
                    new DynamoDBMapper(amazonDynamoDBClient) :
                    new DynamoDBMapper(amazonDynamoDBClient, new DynamoDBMapperConfig(withTableNamePrefix(tablePrefix)));
            final List<Tuple2<String, Long>> collect = rdd.collect();
            for (final Tuple2<String, Long> bucket : collect) {
                createOrUpdate(bucket, dynamoDBMapper);
            }
            return null;
        });
    }

    private static void createOrUpdate(final Tuple2<String, Long> bucket, final DynamoDBMapper dynamoDBMapper) {
        final PaginatedQueryList<EventAggregator> query = dynamoDBMapper.query(EventAggregator.class,
                new DynamoDBQueryExpression<EventAggregator>().withHashKeyValues(
                        new EventAggregator().withBucket(bucket._1())));
        if (query == null || query.isEmpty()) {
            // create
            dynamoDBMapper.save(new EventAggregator(bucket._1(), null, now(), null, bucket._2()));
        } else {
            // update
            // only 1 should exists
            final EventAggregator eventAggregator = query.get(0);
            eventAggregator.setUpdated(now());
            eventAggregator.setCount(eventAggregator.getCount() + bucket._2());
            dynamoDBMapper.save(eventAggregator);
        }
    }

    private static String now() {
        return DateTime.now().toDateTimeISO().toString();
    }
}