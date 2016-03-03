package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.dematic.labs.analytics.common.spark.DriverConfig;
import com.dematic.labs.analytics.common.spark.DriverConsts;
import com.dematic.labs.analytics.common.spark.StreamFunctions.CreateStreamingContextFunction;
import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;
import com.dematic.labs.toolkit.GenericBuilder;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Spliterator;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.ingestion.sparks.drivers.stateful.CycleTimeFunctions.createModel;
import static com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime.TABLE_NAME;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public final class CycleTimeProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CycleTimeProcessor.class);
    public static final String CYCLE_TIME_PROCESSOR_LEASE_TABLE_NAME = TABLE_NAME + "_LT";

    // event stream processing function
    @SuppressWarnings("unchecked")
    private static final class CycleTimeFunction implements VoidFunction<JavaDStream<byte[]>> {
        private final DriverConfig driverConfig;

        public CycleTimeFunction(final DriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(final JavaDStream<byte[]> javaDStream) throws Exception {
            // get the sql context and use it for dataframes
            //final SQLContext sqlContext = SQLContext.getOrCreate(javaDStream.context().sparkContext());

            // transform the byte[] (byte arrays are json) to a string to events and todo: sort by uuid ?
            final JavaDStream<Event> eventStream =
                    javaDStream.map(event -> jsonToEvent(new String(event, Charset.defaultCharset())));

            // group by nodeId
            final JavaPairDStream<String, Multimap<UUID, Event>> nodeToEventsPairs = eventStream.mapToPair(
                    event -> {
                        final Multimap<UUID, Event> nodeToEvents = HashMultimap.create();
                        return Tuple2.apply(event.getNodeId(), nodeToEvents);
                    });

            // reduce to nodeId / events grouped by UUID
            final JavaPairDStream<String, Multimap<UUID, Event>> nodeToEvents = nodeToEventsPairs.reduceByKey(
                    (Function2<Multimap<UUID, Event>, Multimap<UUID, Event>, Multimap<UUID, Event>>)
                            (map1, map2) -> {
                                final Multimap<UUID, Event> nodeToEventsMaps = HashMultimap.create();
                                nodeToEventsMaps.putAll(map1);
                                nodeToEventsMaps.putAll(map2);
                                return nodeToEventsMaps;
                            });

            // create the cycle time Model
            final JavaMapWithStateDStream<String, Multimap<UUID, Event>, CycleTimeState, Optional<CycleTime>>
                    cycleTimeWithState = nodeToEvents.mapWithState(StateSpec.function(new createModel(driverConfig))
                    .timeout(Durations.seconds(30)));// todo: think about timeout...

            cycleTimeWithState.foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {

                    final List<Optional<CycleTime>> collect =
                            stream(spliteratorUnknownSize(partition, Spliterator.CONCURRENT), true)
                                    .collect(Collectors.<Optional<CycleTime>>toList());

                    // just add a flag to be able to turn off reads and writes
                    boolean skipDynamoDBwrite =
                            System.getProperty(DriverConsts.SPARK_DRIVER_SKIP_DYNAMODB_WRITE) != null;
                    if (!skipDynamoDBwrite && !collect.isEmpty()) {
                        writeCycleTimeStateModel(collect, driverConfig);
                    } else {
                        collect.parallelStream().forEach(ct -> LOGGER.debug("CycleTime: >{}<", ct));
                    }
                });
            });
        }

        private static void writeCycleTimeStateModel(final List<Optional<CycleTime>> cycleTimes,
                                                     final DriverConfig driverConfig) {
            final AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient(driverConfig.getDynamoDBEndpoint());
            final DynamoDBMapper dynamoDBMapper = Strings.isNullOrEmpty(driverConfig.getDynamoPrefix()) ?
                    new DynamoDBMapper(dynamoDBClient) : new DynamoDBMapper(dynamoDBClient,
                    new DynamoDBMapperConfig(withTableNamePrefix(driverConfig.getDynamoPrefix())));

            // transform from optional to CT
            final List<CycleTime> collect =
                    cycleTimes.stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());

            if (!collect.isEmpty()) {
                final List<DynamoDBMapper.FailedBatch> failedBatches = dynamoDBMapper.batchSave(collect);
                failedBatches.parallelStream().forEach(failedBatch -> {
                    // for now, not going to retry, just going to log the exception, the next time spark processes a
                    // batch the CT will be saved. That is, all CT calculations are saved in spark state, nothing will
                    // get lost unless there is a jvm crash, this still needs to be worked out
                    LOGGER.error("CycleTime: unprocessed CycleTime model's", failedBatch.getException());
                });
            }
        }

        public static <K, V> Multimap<K, V> mergeMaps(Stream<? extends Multimap<K, V>> stream) {
            return stream.collect(HashMultimap::create, Multimap::putAll, Multimap::putAll);
        }
    }

    public static void main(final String[] args) {
        // master url is only set for testing or running locally
        if (args.length < 4) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint, Kinesis StreamName, DynamoDB " +
                    "Endpoint, optional DynamoDB Prefix, optional driver MasterUrl, driver PollTime");
        }
        final String kinesisEndpoint = args[0];
        final String kinesisStreamName = args[1];
        final String dynamoDBEndpoint = args[2];
        final String dynamoPrefix;
        final String masterUrl;
        final String pollTime;
        if (args.length == 6) {
            dynamoPrefix = args[3];
            masterUrl = args[4];
            pollTime = args[5];
        } else if (args.length == 5) {
            // no master url
            dynamoPrefix = args[3];
            masterUrl = null;
            pollTime = args[4];
        } else {
            // no prefix or master url
            dynamoPrefix = null;
            masterUrl = null;
            pollTime = args[3];
        }

        final String appName = Strings.isNullOrEmpty(dynamoPrefix) ? CYCLE_TIME_PROCESSOR_LEASE_TABLE_NAME :
                String.format("%s%s", dynamoPrefix, CYCLE_TIME_PROCESSOR_LEASE_TABLE_NAME);
        // create the driver configuration and checkpoint dir
        final DriverConfig driverConfig = configure(appName, kinesisEndpoint, kinesisStreamName, dynamoDBEndpoint,
                dynamoPrefix, masterUrl, pollTime);
        driverConfig.setCheckPointDirectoryFromSystemProperties(true);
        // create the table, if it does not exist
        createDynamoTable(driverConfig.getDynamoDBEndpoint(), CycleTime.class, driverConfig.getDynamoPrefix());
        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext = JavaStreamingContext.getOrCreate(driverConfig.getCheckPointDir(),
                new CreateStreamingContextFunction(driverConfig, new CycleTimeFunction(driverConfig)));

        // Start the streaming context and await termination
        LOGGER.info("CT: starting Cycle-Time Processor Driver with master URL >{}<",
                streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("CT: spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();

    }

    private static DriverConfig configure(final String appName, final String kinesisEndpoint,
                                          final String kinesisStreamName, final String dynamoDBEndpoint,
                                          final String dynamoPrefix, final String masterUrl, final String pollTime) {
        return GenericBuilder.of(DriverConfig::new)
                .with(DriverConfig::setAppName, appName)
                .with(DriverConfig::setKinesisEndpoint, kinesisEndpoint)
                .with(DriverConfig::setKinesisStreamName, kinesisStreamName)
                .with(DriverConfig::setDynamoDBEndpoint, dynamoDBEndpoint)
                .with(DriverConfig::setDynamoPrefix, dynamoPrefix)
                .with(DriverConfig::setMasterUrl, masterUrl)
                .with(DriverConfig::setPollTime, pollTime)
                .build();
    }
}
