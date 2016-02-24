package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.dematic.labs.analytics.common.sparks.DriverConfig;
import com.dematic.labs.analytics.common.sparks.DriverConsts;
import com.dematic.labs.analytics.ingestion.sparks.Functions;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTime;
import com.dematic.labs.toolkit.GenericBuilder;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Strings;
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
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.ingestion.sparks.Functions.CreateStreamingContextFunction;
import static com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTime.TABLE_NAME;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

//
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
            final JavaMapWithStateDStream<String, List<Event>, InterArrivalTimeState, InterArrivalTime>
                    mapWithStateDStream = nodeToEvents.mapWithState(
                    StateSpec.function(new Functions.StatefulEventByNodeFunction(driverConfig))
                            .timeout(bufferTimeOut(driverConfig.getBufferTime())));

            mapWithStateDStream.foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {

                    final List<InterArrivalTime> collect = stream(spliteratorUnknownSize(partition,
                            Spliterator.CONCURRENT), true)
                            .collect(Collectors.<InterArrivalTime>toList());

                    // just add a flag to be able to turn off reads and writes
                    boolean skipDynamoDBwrite =
                            System.getProperty(DriverConsts.SPARK_DRIVER_SKIP_DYNAMODB_WRITE) != null;
                    if (!skipDynamoDBwrite && !collect.isEmpty()) {
                        writeInterArrivalTimeStateModel(collect, driverConfig);
                    } else {
                        collect.parallelStream()
                                .forEach(interArrivalTime -> {
                                   // LOGGER.info("IAT: >{}<", interArrivalTime);
                                });
                    }
                });
            });
        }

        private static Duration bufferTimeOut(final String bufferTime) {
            // buffer timeout is the same as the buffer time for now, may have to change, need to test
            return Durations.seconds(Long.valueOf(bufferTime));
        }

        private static void writeInterArrivalTimeStateModel(final List<InterArrivalTime> interArrivalTimes,
                                                            final DriverConfig driverConfig) {
            final AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient(driverConfig.getDynamoDBEndpoint());
            final DynamoDBMapper dynamoDBMapper = Strings.isNullOrEmpty(driverConfig.getDynamoPrefix()) ?
                    new DynamoDBMapper(dynamoDBClient) : new DynamoDBMapper(dynamoDBClient,
                    new DynamoDBMapperConfig(withTableNamePrefix(driverConfig.getDynamoPrefix())));

            // remove any null's
            final List<InterArrivalTime> collect =
                    interArrivalTimes.stream().filter(Objects::nonNull).collect(Collectors.toList());

            if (!collect.isEmpty()) {
                final List<DynamoDBMapper.FailedBatch> failedBatches = dynamoDBMapper.batchSave(collect);
                failedBatches.parallelStream().forEach(failedBatch -> {
                    // for now, not going to retry, just going to log the exception, the next time spark processes a
                    // batch the IAT will be saved. That is, all IAT calculations are saved in spark state, nothing will
                    // get lost unless there is a jvm crash, this still needs to be worked out
                    LOGGER.error("IAT: unprocessed IAT's", failedBatch.getException());
                });
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
