package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.dematic.labs.analytics.common.sparks.DriverConfig;
import com.dematic.labs.analytics.ingestion.sparks.Functions;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTime;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTimeBucket;
import com.dematic.labs.toolkit.GenericBuilder;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.ingestion.sparks.Functions.CreateStreamingContextFunction;
import static com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTime.TABLE_NAME;
import static com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTimeBucket.toInterArrivalTimeBucket;
import static com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTimeUtils.findInterArrivalTime;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;
import static com.dematic.labs.toolkit.communication.EventUtils.dateTime;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;

public final class InterArrivalTimeProcessor implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(InterArrivalTimeProcessor.class);
    public static final String INTER_ARRIVAL_TIME_LEASE_TABLE_NAME = TABLE_NAME + "_LT";

    // event stream processing function
    @SuppressWarnings("unchecked")
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
            final JavaMapWithStateDStream<String, List<Event>, InterArrivalTimeState, InterArrivalTimeStateModel>
                    mapWithStateDStream =
                    nodeToEvents.mapWithState(StateSpec.function(new Functions.StatefulEventByNodeFunction(driverConfig))
                            .timeout(bufferTimeOut(driverConfig.getBufferTime())));
            mapWithStateDStream.foreachRDD(rdd -> {
                // list of node id's and buffered events
                final List<InterArrivalTimeStateModel> collect = rdd.collect();
                collect.parallelStream().forEach(eventsByNode -> {
                    final List<Event> events = eventsByNode.getEvents();
                    if (!events.isEmpty()) {
                        LOGGER.info("IAT: calculating for node {} : event size {}", eventsByNode.getNodeId(),
                                events.size());
                        // calculate inter-arrival time
                        calculateInterArrivalTime(eventsByNode.getNodeId(), events);
                    }
                });
            });
        }

        private static Duration bufferTimeOut(final String bufferTime) {
            // buffer timeout is the same as the buffer time for now, may have to change, need to test
            return Durations.seconds(Long.valueOf(bufferTime));
        }

        private void calculateInterArrivalTime(final String nodeId, final List<Event> events) {
            final AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient(driverConfig.getDynamoDBEndpoint());
            final DynamoDBMapper dynamoDBMapper = Strings.isNullOrEmpty(driverConfig.getDynamoPrefix()) ?
                    new DynamoDBMapper(dynamoDBClient) : new DynamoDBMapper(dynamoDBClient,
                    new DynamoDBMapperConfig(withTableNamePrefix(driverConfig.getDynamoPrefix())));
            // create and populate buckets
            final List<InterArrivalTimeBucket> buckets =
                    createBuckets(Integer.valueOf(driverConfig.getMediumInterArrivalTime()));
            // find inter arrival time from dynamo, used for business logic
            final InterArrivalTime savedInterArrivalTime = findInterArrivalTime(nodeId, dynamoDBMapper);
            // remove and count all errors, that is, events that should have been processed with the last batch
            final List<Event> eventsWithoutErrors = errorChecker(events, savedInterArrivalTime);
            // calculate errors
            long errorCount = events.size() - eventsWithoutErrors.size();
            if (errorCount > 0) {
                LOGGER.error("IAT: node >{}< : errorCount >{}< : events size >{}<", nodeId, errorCount, events.size());
            }
            if (eventsWithoutErrors.size() > 1) {
                // 1) calculate the IAT between batches, if events have been processed already
                if (savedInterArrivalTime != null) {
                    final long interArrivalTimeBetweenBatches =
                            interArrivalTimeBetweenBatches(savedInterArrivalTime.getLastEventTime(),
                                    eventsWithoutErrors);
                    if (interArrivalTimeBetweenBatches == -1) {
                        // error, just update the error count
                        errorCount = errorCount + 1;
                        LOGGER.error("IAT: event time between batches '{}' > event time '{}'",
                                savedInterArrivalTime.getLastEventTime(),
                                eventsWithoutErrors.get(0).getTimestamp().getMillis());
                    } else {
                        // update the bucket
                        addToBucket(interArrivalTimeInSeconds(interArrivalTimeBetweenBatches), buckets);
                    }
                }

                // 2) calculate IAT between events and populate buckets, add all inter arrival times to buckets
                final PeekingIterator<Event> eventPeekingIterator =
                        Iterators.peekingIterator(eventsWithoutErrors.iterator());
                // iterate through the list of events
                for (; eventPeekingIterator.hasNext(); ) {
                    final Event current = eventPeekingIterator.next();
                    if (eventPeekingIterator.hasNext()) {
                        // events r in order
                        final long interArrivalTimeValue =
                                eventPeekingIterator.peek().getTimestamp().getMillis() - current.getTimestamp().getMillis();
                        final long interArrivalTimeValueInSeconds = interArrivalTimeInSeconds(interArrivalTimeValue);
                        addToBucket(interArrivalTimeValueInSeconds, buckets);
                    }
                }
            } else if (eventsWithoutErrors.size() == 1) {
                // only one event
                final Event singleEvent = eventsWithoutErrors.get(0);
                // calculate the inter-arrival time from the last event in dynamoDB
                if (savedInterArrivalTime == null) {
                    // if only 1 event and either no last last event time, just log
                    LOGGER.info("IAT: no previous event to calculate IAT for event {} and nodeId >{}<",
                            singleEvent.toString(), nodeId);
                } else if (interArrivalTimeBetweenBatches(savedInterArrivalTime.getLastEventTime(),
                        eventsWithoutErrors) == -1) {
                    // last event time is >, then current event, just add to the error count
                    errorCount = errorCount + 1;
                    LOGGER.error("IAT: last event time '{}' > then current event time '{}' for node >{}<",
                            savedInterArrivalTime.getLastEventTime(),
                            eventsWithoutErrors.get(0).getTimestamp().getMillis(), nodeId);
                } else {
                    final long interArrivalTimeBetweenBatches =
                            interArrivalTimeBetweenBatches(singleEvent.getTimestamp().getMillis(),
                                    eventsWithoutErrors);
                    addToBucket(interArrivalTimeInSeconds(interArrivalTimeBetweenBatches), buckets);
                }
            } else {
                final String lastEventTime = savedInterArrivalTime != null &&
                        savedInterArrivalTime.getLastEventTime() != null ?
                        dateTime(savedInterArrivalTime.getLastEventTime()).toString() : null;
                // all errors
                LOGGER.error(String.format("IAT: all events for node >%s< within batch are errors - errorCount >%s< " +
                                "batchSize >%s< : last saved IAT >%s< and last batched event time >%s<", nodeId, errorCount,
                        events.size(), lastEventTime, Iterators.getLast(events.iterator()).getTimestamp().toString()));
            }

            // calculate the new lastEventTimeInMillis
            final Long newLastEventTimeInMillis = errorCount == events.size() ? null :
                    eventsWithoutErrors.get(eventsWithoutErrors.size() - 1).getTimestamp().getMillis();

            // create or update
            createOrUpdate(savedInterArrivalTime, nodeId, buckets, newLastEventTimeInMillis, errorCount,
                    dynamoDBMapper);
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

        private static long interArrivalTimeInSeconds(final long interArrivalTimeInMs) {
            return interArrivalTimeInMs / 1000;
        }

        private static List<Event> errorChecker(final List<Event> unprocessedEvents,
                                                final InterArrivalTime interArrivalTime) {
            if (interArrivalTime == null) {
                // nothing to check against
                return unprocessedEvents;
            }
            // get the last event timestamp
            final Long lastEventTime = interArrivalTime.getLastEventTime(); //todo: get from state
            // last event is before new events, just return
            if (lastEventTime == null || lastEventTime < unprocessedEvents.get(0).getTimestamp().getMillis()) {
                return unprocessedEvents;
            }
            // find the fist event in the list that is after the last processed event time
            final java.util.Optional<Event> firstUnprocessedEvent = unprocessedEvents.stream()
                    .filter(event -> lastEventTime < event.getTimestamp().getMillis()).findFirst();

            if (firstUnprocessedEvent.isPresent()) {
                LOGGER.error("IAT: unprocessed events : first event time >{}< last event time >{}<",
                        dateTime(unprocessedEvents.get(0).getTimestamp().getMillis()), dateTime(lastEventTime));

                // remove from the list all events that should have been processed, these are errors
                return unprocessedEvents.subList(unprocessedEvents.indexOf(firstUnprocessedEvent.get()),
                        unprocessedEvents.size());
            }
            // all error, just return empty list
            return Collections.emptyList();
        }

        private static long interArrivalTimeBetweenBatches(final Long lastEventTime, final List<Event> events) {
            if (lastEventTime == null || events == null || events.isEmpty()) {
                return -1;
            }
            // events r in order, if lastEventTime is > then current event, this is an error, just return -1
            final long eventTime = events.get(0).getTimestamp().getMillis();
            return lastEventTime > eventTime ? -1 : eventTime - lastEventTime;
        }

        private static void addToBucket(final long intervalTime, final List<InterArrivalTimeBucket> buckets) {
            final java.util.Optional<InterArrivalTimeBucket> first = buckets.stream()
                    .filter(bucket -> bucket.isWithinBucket(intervalTime)).findFirst();
            if (!first.isPresent()) {
                throw new IllegalStateException(String.format("IAT: - Unexpected Error: intervalTime >%s<" +
                        " not contained within buckets >%s<", intervalTime, buckets));
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
                // add existing and new,
                final List<InterArrivalTimeBucket> updatedBuckets = Lists.newArrayList();
                interArrivalTime.getBuckets().stream()
                        .forEach(bucket -> updatedBuckets.add(toInterArrivalTimeBucket(bucket)));

                newBuckets.stream().forEach(newBucket -> {
                    final int bucketIndex = updatedBuckets.indexOf(newBucket);
                    if (bucketIndex > -1) {
                        final InterArrivalTimeBucket existingInterArrivalTimeBucket =
                                updatedBuckets.remove(bucketIndex);
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
        if (args.length < 6) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint, Kinesis StreamName, DynamoDB " +
                    "Endpoint, optional DynamoDB Prefix, optional driver MasterUrl, driver PollTime, " +
                    "MediumInterArrivalTime, and bufferTime");
        }
        final String kinesisEndpoint = args[0];
        final String kinesisStreamName = args[1];
        final String dynamoDBEndpoint = args[2];
        final String dynamoPrefix;
        final String masterUrl;
        final String pollTime;
        final String mediumInterArrivalTime;
        final String bufferTime;
        if (args.length == 8) {
            dynamoPrefix = args[3];
            masterUrl = args[4];
            pollTime = args[5];
            mediumInterArrivalTime = args[6];
            bufferTime = args[7];
        } else if (args.length == 7) {
            // no master url
            dynamoPrefix = args[3];
            masterUrl = null;
            pollTime = args[4];
            mediumInterArrivalTime = args[5];
            bufferTime = args[6];
        } else {
            // no prefix or master url
            dynamoPrefix = null;
            masterUrl = null;
            pollTime = args[3];
            mediumInterArrivalTime = args[4];
            bufferTime = args[5];
        }

        final String appName = Strings.isNullOrEmpty(dynamoPrefix) ? INTER_ARRIVAL_TIME_LEASE_TABLE_NAME :
                String.format("%s%s", dynamoPrefix, INTER_ARRIVAL_TIME_LEASE_TABLE_NAME);
        // create the driver configuration and checkpoint dir
        final DriverConfig driverConfig = configure(appName, kinesisEndpoint, kinesisStreamName, dynamoDBEndpoint,
                dynamoPrefix, masterUrl, pollTime, mediumInterArrivalTime, bufferTime);
        driverConfig.setCheckPointDirectoryFromSystemProperties(true);
        // create the table, if it does not exist
        createDynamoTable(driverConfig.getDynamoDBEndpoint(), InterArrivalTime.class, driverConfig.getDynamoPrefix());
        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext =
                JavaStreamingContext.getOrCreate(driverConfig.getCheckPointDir(),
                        new CreateStreamingContextFunction(driverConfig, new InterArrivalTimeFunction(driverConfig)));

        // Start the streaming context and await termination
        LOGGER.info("IAT: starting Inter-ArrivalTime Driver with master URL >{}<", streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("IAT: spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }

    private static DriverConfig configure(final String appName, final String kinesisEndpoint,
                                          final String kinesisStreamName, final String dynamoDBEndpoint,
                                          final String dynamoPrefix, final String masterUrl, final String pollTime,
                                          final String mediumInterArrivalTime, final String bufferTime) {
        return GenericBuilder.of(DriverConfig::new)
                .with(DriverConfig::setAppName, appName)
                .with(DriverConfig::setKinesisEndpoint, kinesisEndpoint)
                .with(DriverConfig::setKinesisStreamName, kinesisStreamName)
                .with(DriverConfig::setDynamoDBEndpoint, dynamoDBEndpoint)
                .with(DriverConfig::setDynamoPrefix, dynamoPrefix)
                .with(DriverConfig::setMasterUrl, masterUrl)
                .with(DriverConfig::setPollTime, pollTime)
                .with(DriverConfig::setMediumInterArrivalTime, mediumInterArrivalTime)
                .with(DriverConfig::setBufferTime, bufferTime)
                .build();
    }
}
