package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.clearspring.analytics.util.Lists;
import com.dematic.labs.analytics.common.sparks.DriverConfig;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTime;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTimeBucket;
import com.dematic.labs.toolkit.GenericBuilder;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.ingestion.sparks.Functions.CreateStreamingContextFunction;
import static com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTime.TABLE_NAME;
import static com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTimeBucket.*;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;

public final class InterArrivalTimeProcessor implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(InterArrivalTimeProcessor.class);
    public static final String INTER_ARRIVAL_TIME_LEASE_TABLE_NAME = TABLE_NAME + "_LT";

    // event stream processing function
    private static final class InterArrivalTimeFunction implements VoidFunction<JavaDStream<byte[]>> {
        private final DriverConfig driverConfig;
        public InterArrivalTimeFunction(final DriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(JavaDStream<byte[]> javaDStream) throws Exception {
            // transform the byte[] (byte arrays are json) to a string to events and sort by timestamp
            final JavaDStream<Event> eventStream =
                    javaDStream.map(
                            event -> jsonToEvent(new String(event, Charset.defaultCharset())))
                            .transform(rdd -> rdd.sortBy(event -> event.getTimestamp().getMillis(), true,
                                    rdd.partitions().size()));
            // group by nodeId
            final JavaPairDStream<String, List<Event>> nodeToEventsPairs =
                    eventStream.mapToPair(event -> Tuple2.apply(event.getNodeId(), Collections.singletonList(event)));

            // reduce all events to single node id and determine error cases
            final JavaPairDStream<String, List<Event>> nodeToEvents =
                    nodeToEventsPairs.reduceByKey((events1, events2) -> Stream.of(events1, events2)
                            .flatMap(Collection::stream).collect(Collectors.toList()));
            //todo: look into update by key, last event date, when spark 1.6 is out
            // calculate inter-arrival time
            nodeToEvents.foreachRDD(rdd -> {
                rdd.collect().forEach(eventsByNode -> {
                    calculateInterArrivalTime(eventsByNode._1(), eventsByNode._2());
                    LOGGER.info("node {} : event size {}", eventsByNode._1(), eventsByNode._2().size());
                });
                return null;
            });
        }

        private void calculateInterArrivalTime(final String nodeId, final List<Event> events) {
            final AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient(driverConfig.getDynamoDBEndpoint());
            final DynamoDBMapper dynamoDBMapper = Strings.isNullOrEmpty(driverConfig.getDynamoPrefix()) ?
                    new DynamoDBMapper(dynamoDBClient) : new DynamoDBMapper(dynamoDBClient,
                    new DynamoDBMapperConfig(withTableNamePrefix(driverConfig.getDynamoPrefix())));


            // find inter arrival time from dynamo, used for business logic
            final InterArrivalTime interArrivalTime = findInterArrivalTime(nodeId, dynamoDBMapper);
            // remove and count all errors, that is, events that should have been processed with the last batch
            final List<Event> eventsWithoutErrors = errorChecker(events, interArrivalTime);
            // calculate errors
            final long errorCount = events.size() - eventsWithoutErrors.size();
            // calculate the new lastEventTimeInMillis
            final Long newLastEventTimeInMillis = errorCount == events.size() ? null :
                    events.get(events.size() - 1).getTimestamp().getMillis();
            // create and populate buckets
            final List<InterArrivalTimeBucket> buckets =
                    createBuckets(Integer.valueOf(driverConfig.getMediumInterArrivalTime()));
            // populate buckets, add all inter arrival times to buckets
            final PeekingIterator<Event> eventPeekingIterator =
                    Iterators.peekingIterator(eventsWithoutErrors.iterator());
            for (; eventPeekingIterator.hasNext(); ) {
                final Event current = eventPeekingIterator.next();
                if (eventPeekingIterator.hasNext()) {
                    // events r in order
                    final long interArrivalTimeValue =
                            eventPeekingIterator.peek().getTimestamp().getMillis() - current.getTimestamp().getMillis();
                    addToBucket(interArrivalTimeValue, buckets);
                } else {
                    // deal with last event
                    //todo: implement
                    LOGGER.error("still need to calculate last event inter arrival time");
                }
            }
            // create or update
            createOrUpdate(interArrivalTime, nodeId, buckets, newLastEventTimeInMillis, errorCount, dynamoDBMapper);
        }

        private static InterArrivalTime findInterArrivalTime(final String nodeId, final DynamoDBMapper dynamoDBMapper) {
            // lookup buckets by nodeId
            final PaginatedQueryList<InterArrivalTime> query = dynamoDBMapper.query(InterArrivalTime.class,
                    new DynamoDBQueryExpression<InterArrivalTime>()
                            .withHashKeyValues(new InterArrivalTime(nodeId)));
            if (query == null || query.isEmpty()) {
                return null;
            }
            // only 1 should exists
            return query.get(0);
        }

        private static List<Event> errorChecker(final List<Event> unprocessedEvents,
                                                final InterArrivalTime interArrivalTime) {
            if (interArrivalTime == null) {
                // nothing to check against
                return unprocessedEvents;
            }
            // get the last event timestamp
            final Long lastEventTime = interArrivalTime.getLastEventTime();
            // last event is before new events, just return
            if (lastEventTime == null || lastEventTime < unprocessedEvents.get(0).getTimestamp().getMillis()) {
                return unprocessedEvents;
            }
            // find the fist event in the list that is after the last processed event time
            final Optional<Event> firstUnprocessedEvent = unprocessedEvents.stream()
                    .filter(event -> lastEventTime < event.getTimestamp().getMillis()).findFirst();

            if (firstUnprocessedEvent.isPresent()) {
                // remove from the list all events that should have been processed, these are errors
                return unprocessedEvents.subList(unprocessedEvents.indexOf(firstUnprocessedEvent.get()),
                        unprocessedEvents.size());
            }
            // all error, just return empty list
            return Collections.emptyList();
        }

        private static List<InterArrivalTimeBucket> createBuckets(final int avgInterArrivalTime) {
            final List<InterArrivalTimeBucket> buckets = Lists.newArrayList();
            // see https://docs.google.com/document/d/1J9mSW8EbxTwbsGGeZ7b8TVkF5lm8-bnjy59KpHCVlBA/edit# for specs
            for (int i = 0; i < avgInterArrivalTime * 2; i++) {
                final int low = i * avgInterArrivalTime / 5;
                final int high = (i + 1) * avgInterArrivalTime / 5;
                if (high > avgInterArrivalTime * 2) {
                    // add the last bucket
                    buckets.add(new InterArrivalTimeBucket(low, Integer.MAX_VALUE, 0L));
                    break;
                }
                buckets.add(new InterArrivalTimeBucket(low, high, 0L));
            }
            return buckets;
        }

        // todo: needs more testing
        private static void addToBucket(final long intervalTime, final List<InterArrivalTimeBucket> buckets) {
            final Optional<InterArrivalTimeBucket> first = buckets.stream()
                    .filter(bucket -> bucket.isWithinBucket(intervalTime)).findFirst();
            if (!first.isPresent()) {
                throw new IllegalStateException(String.format("InterArrivalTime - Unexpected Error: intervalTime >%s<" +
                        " not contained within buckets >%s<", intervalTime, buckets)); //todo: better logging
            }
            final InterArrivalTimeBucket interArrivalTimeBucket = first.get();
            interArrivalTimeBucket.incrementCount();
        }

        private static void createOrUpdate(final InterArrivalTime interArrivalTime, final String nodeId,
                                           final List<InterArrivalTimeBucket> newBuckets,
                                           final Long lastEventTimeInMillis, final Long errorCount,
                                           final DynamoDBMapper dynamoDBMapper) {
            if (interArrivalTime == null) {
                // create
                final InterArrivalTime newInterArrivalTime = new InterArrivalTime(nodeId);
                if (lastEventTimeInMillis != null) {
                    newInterArrivalTime.setLastEventTime(lastEventTimeInMillis);
                }
                final Set<String> bucketsString = Sets.newHashSet();
                newBuckets.stream().forEach(bucket -> bucketsString.add(bucket.toJson()));
                newInterArrivalTime.setBuckets(bucketsString);
                newInterArrivalTime.setErrorCount(errorCount);
                dynamoDBMapper.save(newInterArrivalTime);
            } else {
                // update
                if (lastEventTimeInMillis != null) {
                    interArrivalTime.setLastEventTime(lastEventTimeInMillis);
                }

                // add existing and new
                final List<InterArrivalTimeBucket> updatedBuckets = Lists.newArrayList();
                interArrivalTime.getBuckets().stream()
                        .forEach(bucket -> updatedBuckets.add(toInterArrivalTimeBucket(bucket)));

                newBuckets.stream().forEach(newBucket -> {
                    final int bucketIndex = updatedBuckets.indexOf(newBucket);
                    if (bucketIndex > -1) {
                        final InterArrivalTimeBucket existingInterArrivalTimeBucket = updatedBuckets.get(bucketIndex);
                        // add the counts
                        final long existingCount = existingInterArrivalTimeBucket.getCount();
                        final long newCount = newBucket.getCount();
                        existingInterArrivalTimeBucket.setCount(existingCount + newCount);
                        updatedBuckets.add(existingInterArrivalTimeBucket);
                    } else {
                        // new bucket
                        updatedBuckets.add(newBucket);
                    }
                });

                final Set<String> bucketsString = Sets.newHashSet();
                updatedBuckets.stream().forEach(bucket -> bucketsString.add(bucket.toJson()));

                interArrivalTime.setBuckets(bucketsString);
                final Long existingErrorCount = interArrivalTime.getErrorCount();
                interArrivalTime.setErrorCount(existingErrorCount + errorCount);
                dynamoDBMapper.save(interArrivalTime);
            }
        }
    }

    // functions
    public static void main(final String[] args) {
        // master url is only set for testing or running locally
        if (args.length < 5) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint, Kinesis StreamName, DynamoDB " +
                    "Endpoint, optional DynamoDB Prefix, optional driver MasterUrl, driver PollTime, and " +
                    "MediumInterArrivalTime");
        }
        final String kinesisEndpoint = args[0];
        final String kinesisStreamName = args[1];
        final String dynamoDBEndpoint = args[2];
        final String dynamoPrefix;
        final String masterUrl;
        final String pollTime;
        final String mediumInterArrivalTime;
        if (args.length == 7) {
            dynamoPrefix = args[3];
            masterUrl = args[4];
            pollTime = args[5];
            mediumInterArrivalTime = args[6];
        } else if (args.length == 6) {
            // no master url
            dynamoPrefix = args[3];
            masterUrl = null;
            pollTime = args[4];
            mediumInterArrivalTime = args[5];
        } else {
            // no prefix or master url
            dynamoPrefix = null;
            masterUrl = null;
            pollTime = args[3];
            mediumInterArrivalTime = args[4];
        }

        final String appName = Strings.isNullOrEmpty(dynamoPrefix) ? INTER_ARRIVAL_TIME_LEASE_TABLE_NAME :
                String.format("%s%s", dynamoPrefix, INTER_ARRIVAL_TIME_LEASE_TABLE_NAME);
        // create the driver configuration and checkpoint dir
        final DriverConfig driverConfig = configure(appName, kinesisEndpoint, kinesisStreamName, dynamoDBEndpoint,
                dynamoPrefix, masterUrl, pollTime, mediumInterArrivalTime);
        driverConfig.setCheckPointDirectoryFromSystemProperties(true);
        // create the table, if it does not exist
        createDynamoTable(driverConfig.getDynamoDBEndpoint(), InterArrivalTime.class, driverConfig.getDynamoPrefix());
        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext =
                JavaStreamingContext.getOrCreate(driverConfig.getCheckPointDir(),
                        new CreateStreamingContextFunction(driverConfig, new InterArrivalTimeFunction(driverConfig)));

        // Start the streaming context and await termination
        LOGGER.info("starting Inter-ArrivalTime Driver with master URL >{}<", streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }

    private static DriverConfig configure(final String appName, final String kinesisEndpoint,
                                          final String kinesisStreamName, final String dynamoDBEndpoint,
                                          final String dynamoPrefix, final String masterUrl, final String pollTime,
                                          final String mediumInterArrivalTime) {
        return GenericBuilder.of(DriverConfig::new)
                .with(DriverConfig::setAppName, appName)
                .with(DriverConfig::setKinesisEndpoint, kinesisEndpoint)
                .with(DriverConfig::setKinesisStreamName, kinesisStreamName)
                .with(DriverConfig::setDynamoDBEndpoint, dynamoDBEndpoint)
                .with(DriverConfig::setDynamoPrefix, dynamoPrefix)
                .with(DriverConfig::setMasterUrl, masterUrl)
                .with(DriverConfig::setPollTime, pollTime)
                .with(DriverConfig::setMediumInterArrivalTime, mediumInterArrivalTime)
                .build();
    }
}
