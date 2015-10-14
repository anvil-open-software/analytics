package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.util.StringUtils;
import com.dematic.labs.analytics.ingestion.sparks.tables.EventAggregator;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Strings;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getStreamingContext;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getCacheClient;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;
import static com.dematic.labs.toolkit.communication.EventUtils.nowString;

public final class EventStreamCacheAggregator implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamCacheAggregator.class);
    private static final int MAX_RETRY = 3;

    // functions
    public static final class AggregateEvent implements PairFunction<Event, String, Long> {
        private final TimeUnit timeUnit;

        public AggregateEvent(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        @Override
        public Tuple2<String, Long> call(final Event event) throws Exception {
            // Downsampling: where we reduce the event’s ISO 8601 timestamp down to timeUnit precision,
            // so for instance “2015-06-05T12:54:43.064528” becomes “2015-06-05T12:54:00.000000” for minute.
            // This downsampling gives us a fast way of bucketing or aggregating events via this downsampled key
            return new Tuple2<>(event.aggregateBy(timeUnit), 1L);
        }
    }

    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

    public static final String EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME = EventAggregator.TABLE_NAME + "_Cache_LT";

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
        final EventStreamCacheAggregator eventStreamAggregator = new EventStreamCacheAggregator();
        eventStreamAggregator.aggregateEvents(getJavaDStream(kinesisEndpoint, streamName, streamingContext),
                dynamoDBEndpoint, dynamoPrefix, timeUnit);

        streamingContext.start();
        LOGGER.info("spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }

    public void aggregateEvents(final JavaDStream<byte[]> byteStream, final String dynamoDBEndpoint,
                                final String tablePrefix, final TimeUnit timeUnit) {

        // transform the byte[] (byte arrays are json) to a string to events, and ensure distinct within stream and
        // filter against cache to ensure no duplicates
        final JavaDStream<Event> eventStream =
                byteStream.map(
                        event -> jsonToEvent(new String(event, Charset.defaultCharset()))
                ).transform((Function<JavaRDD<Event>, JavaRDD<Event>>) JavaRDD::distinct)
                        .filter(event -> {
                            final Jedis cacheClient = getCacheClient("", 1234);
                            final String uuid = event.getEventId().toString();
                            if (cacheClient.exists(uuid)) {
                                return false;
                            } else {
                                cacheClient.set(uuid, uuid, "NX", "EX", Seconds.seconds(3600).getSeconds());
                                return true;
                            }
                        });

        // map to pairs and aggregate by key
        final JavaPairDStream<String, Long> aggregates = eventStream
                .mapToPair(new AggregateEvent(timeUnit))
                .reduceByKey(SUM_REDUCER);

        // save counts
        aggregates.foreachRDD(rdd -> {
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
        int count = 1;
        do {
            EventAggregator eventAggregator = null;
            try {

                final PaginatedQueryList<EventAggregator> query = dynamoDBMapper.query(EventAggregator.class,
                        new DynamoDBQueryExpression<EventAggregator>().withHashKeyValues(
                                new EventAggregator().withBucket(bucket._1())));
                if (query == null || query.isEmpty()) {
                    // create
                    eventAggregator = new EventAggregator(bucket._1(), null, nowString(), null, bucket._2(), null);
                    dynamoDBMapper.save(eventAggregator);
                    break;
                } else {
                    // update
                    // only 1 should exists
                    eventAggregator = query.get(0);
                    eventAggregator.setUpdated(nowString());
                    eventAggregator.setCount(eventAggregator.getCount() + bucket._2());
                    dynamoDBMapper.save(eventAggregator);
                    break;
                }
            } catch (final Throwable any) {
                LOGGER.error("unable to save >{}< trying again {}", eventAggregator, count, any);
            } finally {
                count++;
            }
        } while (count <= MAX_RETRY);
    }
}
