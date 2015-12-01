package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.analytics.common.sparks.DriverConfig;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTimeBucket;
import com.dematic.labs.toolkit.GenericBuilder;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dematic.labs.analytics.ingestion.sparks.Functions.CreateStreamingContextFunction;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;

public final class InterArrivalTimeProcessor implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(InterArrivalTimeProcessor.class);
    public static final String INTER_ARRIVAL_TIME_LEASE_TABLE_NAME = InterArrivalTimeBucket.TABLE_NAME + "_LT";

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
                            .flatMap(Collection::stream).collect(Collectors.toList()))
                            .updateStateByKey(
                                    (Function2<List<List<Event>>, Optional<List<Event>>, Optional<List<Event>>>)
                                            (currentEvents, existingEvent) -> {
                                                //todo : ensure in order
                                                // current events r in order
                                                final List<Event> totalCurrent = currentEvents.stream()
                                                        .flatMap(Collection::stream).collect(Collectors.toList());
                                                // no existing event
                                                if (currentEvents.isEmpty()) {
                                                    // todo: do we just return the existing
                                                    return existingEvent.isPresent() ? existingEvent : Optional.absent();
                                                }
                                                if (!existingEvent.isPresent()) {
                                                    // no existing, just save the last event
                                                    return Optional.of(Collections.singletonList(
                                                            totalCurrent.get(totalCurrent.size() - 1)));
                                                }
                                                // ensure all current events are after existing, otherwise error case

                                                final Event firstCurrent = totalCurrent.get(0);


                                                System.out.println(currentEvents);
                                                System.out.println(totalCurrent);
                                                System.out.println(existingEvent);

                                                System.out.println();
                                                return Optional.of(Collections.emptyList());
                                            });
            // look into update by key, last event date

            // calculate inter-arrival time
            nodeToEvents.foreachRDD(rdd -> {
                rdd.collect().forEach(eventsByNode -> {
                    calculateInterArrivalTime(eventsByNode._1(), eventsByNode._2());
                    LOGGER.info("node {} : event size {}", eventsByNode._1(), eventsByNode._2().size());
                });
                return null;
            });
        }

        private static void calculateInterArrivalTime(final String nodeId, final List<Event> orderedEvents) {
            final PeekingIterator<Event> eventPeekingIterator = Iterators.peekingIterator(orderedEvents.iterator());
            for (; eventPeekingIterator.hasNext(); ) {
                final Event current = eventPeekingIterator.next();
                if (eventPeekingIterator.hasNext()) {
                    // events r in order
                    final long interArrivalTime =
                            eventPeekingIterator.peek().getTimestamp().getMillis() - current.getTimestamp().getMillis();
                    // save to db
                }
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
        createDynamoTable(driverConfig.getDynamoDBEndpoint(), InterArrivalTimeBucket.class, driverConfig.getDynamoPrefix());
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
