package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.common.spark.DriverConfig;
import com.dematic.labs.analytics.common.spark.StreamFunctions.CreateStreamingContextFunction;
import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;

import com.dematic.labs.toolkit.GenericBuilder;
import com.google.common.base.Strings;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime.TABLE_NAME;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;

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
        final JavaStreamingContext streamingContext =
                JavaStreamingContext.getOrCreate(driverConfig.getCheckPointDir(),
                        new CreateStreamingContextFunction(driverConfig, new CycleTimeFunction(driverConfig)));

        // Start the streaming context and await termination
        LOGGER.info("IAT: starting Cycle-Time Processor Driver with master URL >{}<",
                streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("IAT: spark state: {}", streamingContext.getState().name());
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
