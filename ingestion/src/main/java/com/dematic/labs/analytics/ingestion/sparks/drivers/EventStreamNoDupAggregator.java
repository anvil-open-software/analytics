package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.util.StringUtils;
import com.dematic.labs.analytics.ingestion.sparks.tables.EventAggregator;
import com.dematic.labs.analytics.ingestion.sparks.tables.EventAggregatorBucket;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getStreamingContext;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;
import static com.dematic.labs.toolkit.communication.EventUtils.nowString;

public final class EventStreamNoDupAggregator {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamNoDupAggregator.class);

    // functions
    public static final class AggregateEvents implements Function2<List<String>, List<String>, List<String>> {
        @Override
        public List<String> call(final List<String> aggregateEventsOne, final List<String> aggregateEventsTwo)
                throws Exception {
            return Stream.concat(aggregateEventsOne.stream(), aggregateEventsTwo.stream()).collect(Collectors.toList());
        }
    }

    public static final class AggregateEvent implements PairFunction<Event, String, List<String>> {
        private final TimeUnit timeUnit;

        public AggregateEvent(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        // Downsampling: where we reduce the event’s ISO 8601 timestamp down to timeUnit precision,
        // so for instance “2015-06-05T12:54:43.064528” becomes “2015-06-05T12:54:00.000000” for minute.
        // This downsampling gives us a fast way of bucketing or aggregating events via this downsampled key
        @Override
        public Tuple2<String, List<String>> call(final Event event) throws Exception {
            return new Tuple2<>(event.aggregateBy(timeUnit), Collections.singletonList(event.getEventId().toString()));
        }
    }

    private static final int MAX_RETRY = 3;
    private static final int MAX_CHUNK_SIZE = 18000;

    public static final String EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME = EventAggregator.TABLE_NAME + "_NoDup_LT";

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
        createDynamoTable(dynamoDBEndpoint, EventAggregatorBucket.class, dynamoPrefix);
        //todo: master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext = getStreamingContext(null, appName, null, pollTime);

        // Start the streaming context and await termination
        LOGGER.info("starting Event No Dup Aggregator Driver with master URL >{}<",
                streamingContext.sparkContext().master());
        final EventStreamNoDupAggregator eventStreamAggregator = new EventStreamNoDupAggregator();
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
        final JavaPairDStream<String, List<String>> aggregateBucket =
                eventStream.mapToPair(new AggregateEvent(timeUnit)).reduceByKey(new AggregateEvents());

        // save counts
        aggregateBucket.foreachRDD(rdd -> {
            final AmazonDynamoDBClient amazonDynamoDBClient = Connections.getAmazonDynamoDBClient(dynamoDBEndpoint);
            final DynamoDBMapper dynamoDBMapper = StringUtils.isNullOrEmpty(tablePrefix) ?
                    new DynamoDBMapper(amazonDynamoDBClient) :
                    new DynamoDBMapper(amazonDynamoDBClient, new DynamoDBMapperConfig(withTableNamePrefix(tablePrefix)));

            rdd.collectAsMap().forEach((bucket, eventsToCount) -> createOrUpdate(bucket, eventsToCount, dynamoDBMapper));
            return null;
        });
    }

    private static void createOrUpdate(final String bucket, final List<String> eventsToCount,
                                       final DynamoDBMapper dynamoDBMapper) {
        int count = 1;
        do {
            try {
                final PaginatedQueryList<EventAggregator> query = dynamoDBMapper.query(EventAggregator.class,
                        new DynamoDBQueryExpression<EventAggregator>().withHashKeyValues(
                                new EventAggregator().withBucket(bucket)));
                // todo: deal with multiple table updates
                if (query == null || query.isEmpty()) {
                    if (eventsToCount.size() > MAX_CHUNK_SIZE) {
                        int chunkCount = 1;
                        final List<List<String>> chunks = Lists.partition(eventsToCount, MAX_CHUNK_SIZE);
                        // save aggregation
                        dynamoDBMapper.save(new EventAggregator(bucket, null, nowString(), null,
                                (long) eventsToCount.size(), (long) chunks.size()));
                        for (final List<String> chunk : chunks) {
                            // save bucket
                            dynamoDBMapper.save(new EventAggregatorBucket(bucketKey(bucket, chunkCount++),
                                    compress(chunk)));
                        }
                    } else {
                        // save aggregation
                        dynamoDBMapper.save(new EventAggregator(bucket, null, nowString(), null,
                                  (long) eventsToCount.size(), null));
                        // save bucket
                        dynamoDBMapper.save(new EventAggregatorBucket(bucketKey(bucket, 1), compress(eventsToCount)));
                    }
                    break;
                } else {
                    // update
                    // only 1 should exists
                    final EventAggregator eventAggregator = query.get(0);
                    eventAggregator.setUpdated(nowString());
                 //   eventAggregator.setCount(eventAggregator.getCount() + bucket._2());
                    dynamoDBMapper.save(eventAggregator);
                    break;
                }
            } catch (final Throwable any) {
              //  LOGGER.error("unable to save >{}< trying again {}", eventAggregator, count, any);
            } finally {
                count++;
            }
        } while (count <= MAX_RETRY);
    }

    private static String bucketKey(final String key, final int count) {
        return String.format("%s#%s", key, count);
    }

    private static byte[] compress(final List<String> uuidChunk) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
        try (ObjectOutputStream objectOut = new ObjectOutputStream(gzipOut)) {
            objectOut.writeObject(uuidChunk);
        } finally {
            gzipOut.finish();
            baos.flush();
        }
        return baos.toByteArray();
    }

    private static List<String> uncompress(final byte[] uuidChunk) throws IOException, ClassNotFoundException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(uuidChunk);
        final GZIPInputStream gzipIn = new GZIPInputStream(bais);
        final List<String> uncompressed;
        try (ObjectInputStream objectIn = new ObjectInputStream(gzipIn)) {
            //noinspection unchecked
            uncompressed  = (List<String>) objectIn.readObject();
        }
        return uncompressed;
    }
}
