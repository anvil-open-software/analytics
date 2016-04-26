package com.dematic.labs.analytics.ingestion.spark.drivers.stateless;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.util.StringUtils;
import com.dematic.labs.analytics.ingestion.spark.tables.EventAggregator;
import com.dematic.labs.analytics.ingestion.spark.tables.EventAggregatorBucket;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.common.spark.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.spark.DriverUtils.getStreamingContext;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;
import static com.dematic.labs.toolkit.communication.EventUtils.nowString;

public final class EventStreamNoDupAggregator {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamNoDupAggregator.class);

    // functions
    private static final class AggregateEvents implements Function2<Collection<String>, Collection<String>,
            Collection<String>> {
        @Override
        public Collection<String> call(final Collection<String> aggregateEventsOne,
                                       final Collection<String> aggregateEventsTwo) throws Exception {
            return Stream.concat(aggregateEventsOne.stream(), aggregateEventsTwo.stream()).collect(Collectors.toSet());
        }
    }

    private static final class AggregateEvent implements PairFunction<Event, String, Collection<String>> {
        private final TimeUnit timeUnit;

        AggregateEvent(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        // Downsampling: where we reduce the event’s ISO 8601 timestamp down to timeUnit precision,
        // so for instance “2015-06-05T12:54:43.064528” becomes “2015-06-05T12:54:00.000000” for minute.
        // This downsampling gives us a fast way of bucketing or aggregating events via this downsampled key
        @Override
        public Tuple2<String, Collection<String>> call(final Event event) throws Exception {
            return new Tuple2<>(event.aggregateBy(timeUnit), Collections.singleton(event.getId().toString()));
        }
    }

    private static final int MAX_RETRY = 3;
    private static final int MAX_CHUNK_SIZE = 18000;

    private static final String EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME = EventAggregator.TABLE_NAME + "_NoDup_LT";

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

    private void aggregateEvents(final JavaDStream<byte[]> byteStream, final String dynamoDBEndpoint,
                                 final String tablePrefix, final TimeUnit timeUnit) {

        // transform the byte[] (byte arrays are json) to a string to events, and ensure distinct within stream
        final JavaDStream<Event> eventStream =
                byteStream.map(
                        event -> jsonToEvent(new String(event, Charset.defaultCharset()))
                ).transform((Function<JavaRDD<Event>, JavaRDD<Event>>) JavaRDD::distinct);

        // map to pairs and aggregate by key
        final JavaPairDStream<String, Collection<String>> aggregateBucket =
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

    private static void createOrUpdate(final String bucket, final Collection<String> eventsToCount,
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
                        // break events into chunks of events and save
                        final Tuple2<List<EventAggregatorBucket>, Long> chunks = chunkEvents(bucket, eventsToCount);
                        // save aggregation
                        dynamoDBMapper.save(new EventAggregator(bucket, null, nowString(), null,
                                (long) eventsToCount.size(), chunks._2()));
                        saveBuckets(chunks._1(), dynamoDBMapper);
                    } else {
                        // save aggregation
                        dynamoDBMapper.save(new EventAggregator(bucket, null, nowString(), null, (long) eventsToCount.size(), 1L));
                        // save bucket
                        dynamoDBMapper.save(new EventAggregatorBucket(bucketKey(bucket, 1), compress(eventsToCount)));
                    }
                    break;
                } else {
                    // update
                    // only 1 should exists
                    final EventAggregator eventAggregator = query.get(0);
                    // get existing events and new events to get accurate count
                    @SuppressWarnings("unchecked") final Set<String> noDupUUIDs =
                            newCount(eventAggregator, (Collection)eventsToCount, dynamoDBMapper);
                    // determine if we need to chuck events
                    if (noDupUUIDs.size() > MAX_CHUNK_SIZE) {
                        final Tuple2<List<EventAggregatorBucket>, Long> chunks = chunkEvents(bucket, noDupUUIDs);
                        eventAggregator.setUpdated(nowString());
                        // new count
                        eventAggregator.setCount((long) noDupUUIDs.size());
                        // new chunk size
                        eventAggregator.setChunk(chunks._2());
                        // save aggregation
                        dynamoDBMapper.save(eventAggregator);
                        // save buckets
                        saveBuckets(chunks._1(), dynamoDBMapper);
                    } else {
                        eventAggregator.setUpdated(nowString());
                        // new count
                        eventAggregator.setCount((long) noDupUUIDs.size());
                        // save aggregation
                        dynamoDBMapper.save(eventAggregator);
                        // save bucket, just override existing
                        dynamoDBMapper.save(new EventAggregatorBucket(bucketKey(bucket, 1), compress(noDupUUIDs)));
                    }
                    break;
                }
            } catch (final Throwable any) {
                  LOGGER.error("unexpected error trying to create or update aggregates", any);
            } finally {
                count++;
            }
        } while (count <= MAX_RETRY);
    }

    private static Tuple2<List<EventAggregatorBucket>, Long> chunkEvents(final String bucket,
                                                                         final Collection<String> eventsToCount)
            throws IOException {
        int chunkCount = 1;
        final Iterable<List<String>> chunks = Iterables.partition(eventsToCount, MAX_CHUNK_SIZE);
        final int chuckSize = Iterables.size(chunks);

        final List<EventAggregatorBucket> eventAggregatorBuckets = new ArrayList<>(chuckSize);
        // todo: looking making function
        for (final List<String> chunk : chunks) {
            eventAggregatorBuckets.add(new EventAggregatorBucket(bucketKey(bucket, chunkCount++), compress(chunk)));
        }
        return new Tuple2<>(eventAggregatorBuckets, (long) chunkCount - 1);
    }

    private static void saveBuckets(final List<EventAggregatorBucket> aggregatorBuckets,
                                    final DynamoDBMapper dynamoDBMapper) {
        int count = 0;
        List<DynamoDBMapper.FailedBatch> failedBatches;
        do {
            failedBatches = dynamoDBMapper.batchSave(aggregatorBuckets);
        } while (!failedBatches.isEmpty() && count++ < MAX_RETRY);

        //todo: figure out what to do
        if (!failedBatches.isEmpty()) {
            throw new IllegalStateException(String.format("unable to save the Event Aggregate Buckets >%s<",
                    failedBatches));
        }
    }

    // only called for updates when chunk exist
    private static Set<String> newCount(final EventAggregator eventAggregator, final Collection<Object> eventsToCount,
                                        final DynamoDBMapper dynamoDBMapper) {
        final String bucket = eventAggregator.getBucket();
        final long chunk = eventAggregator.getChunk();
        // create the keys and Objects to load
        final List<Object> eventAggregatorBuckets = new ArrayList<>((int) chunk);
        for (int i = 1; i <= chunk; i++) {
            eventAggregatorBuckets.add(new EventAggregatorBucket(bucketKey(bucket, i), null));
        }
        // load existing uuids
        final Map<String, List<Object>> uuidMap = dynamoDBMapper.batchLoad(eventAggregatorBuckets);
        // existing uuids
        @SuppressWarnings("unchecked")
        final Collection<List<EventAggregatorBucket>> eventAggregatorBucketList = (Collection) uuidMap.values();
        // collect all the eventAggregatorBuckets and transform to a set and add new uuids
        return eventAggregatorBucketList.stream().map(eventAggregatorBucket -> {
            @SuppressWarnings("unchecked") final Set<String> uuidChunks = new HashSet<>((Collection) eventsToCount);
            eventAggregatorBucket.forEach(uuidChunk -> {
                try {
                    uuidChunks.addAll(uncompress(uuidChunk.getUuids()));
                } catch (final Throwable any) {
                    throw new IllegalStateException("Unexpected Exception getting existing UUID's", any);
                }
            });
            return uuidChunks;
            // return the new set without duplicates
        }).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    private static String bucketKey(final String key, final long count) {
        return String.format("%s#%s", key, count);
    }

    private static byte[] compress(final Collection<String> uuidChunk) throws IOException {
        final HashSet<String> uuidSet = new HashSet<>(uuidChunk);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
        try (ObjectOutputStream objectOut = new ObjectOutputStream(gzipOut)) {
            objectOut.writeObject(uuidSet);
        } finally {
            try {
                gzipOut.finish();
            } catch (final Throwable ignore) {
            }
            try {
                baos.flush();
            } catch (final Throwable ignore) {
            }
        }
        return baos.toByteArray();
    }

    private static Collection<String> uncompress(final byte[] uuidChunk) throws IOException, ClassNotFoundException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(uuidChunk);
        final GZIPInputStream gzipIn = new GZIPInputStream(bais);
        final Collection<String> uncompressed;
        try (ObjectInputStream objectIn = new ObjectInputStream(gzipIn)) {
            //noinspection unchecked
            uncompressed = (Collection<String>) objectIn.readObject();
        }
        return uncompressed;
    }
}
