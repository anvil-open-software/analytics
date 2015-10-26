package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.analytics.common.sparks.DematicSparkSession;
import com.dematic.labs.analytics.common.sparks.DriverUtils;
import com.dematic.labs.analytics.ingestion.sparks.tables.EventAggregator;
import com.google.common.base.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;

/**
 * Wrapper for EventStreamAggregator with checkpoint turned on. Does not have any stateful processing.
 * <p>
 * Probably could have just turned on checkpointing in EventStreamAggregator
 * Also the argument parsing probably could be refactored to be shared among all EventStreamAggregators
 */
public final class EventStreamCheckpointedAggregator implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamCheckpointedAggregator.class);

    public static final String EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME = EventAggregator.TABLE_NAME + "_Checkpoint_LT";

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

        DematicSparkSession session = new DematicSparkSession(appName, kinesisEndpoint, streamName);
        // checkpoint dir comes from the jvm params
        session.setCheckPointDirectoryFromSystemProperties(true);

        // create the table, if it does not exist
        createDynamoTable(dynamoDBEndpoint, EventAggregator.class, dynamoPrefix);
        final JavaStreamingContext streamingContext = initializeCheckpointedSparkSession(session, null, pollTime);

        // Start the streaming context and await termination
        LOGGER.info("starting Event Aggregator Driver with master URL >{}<", streamingContext.sparkContext().master());
        final EventStreamAggregator eventStreamAggregator = new EventStreamAggregator();
        LOGGER.info("DStreams: " + session.getDStreams().toString());
        eventStreamAggregator.aggregateEvents(session.getDStreams(), dynamoDBEndpoint, dynamoPrefix, timeUnit);

        streamingContext.start();
        LOGGER.info("spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }

    /**
     * To checkpoint, need to create the stream inside the factory before calling checkpoint.
     */
    public static JavaStreamingContext initializeCheckpointedSparkSession(final DematicSparkSession session,
                                                                          final String masterUrl,
                                                                          final Duration pollTime) {
        final String checkPointDir = session.getCheckPointDir();
        final JavaStreamingContextFactory factory = () -> {
            // Spark config
            final SparkConf configuration = new SparkConf().
                    // sets the lease manager table name
                            setAppName(session.getAppName());
            if (!Strings.isNullOrEmpty(masterUrl)) {
                configuration.setMaster(masterUrl);
            }
            final JavaStreamingContext streamingContext = new JavaStreamingContext(configuration, pollTime);
            // we must now create kinesis streams before we checkpoint
            LOGGER.info("Creating Kinesis DStreams for " + session.getStreamName());
            JavaDStream kinesisDStream = DriverUtils.getJavaDStream(session.getAwsEndPoint(), session.getStreamName(), streamingContext);
            session.setDStreams(kinesisDStream);
            LOGGER.info("Created DStream:  " + kinesisDStream);

            LOGGER.info("Checkpointing to " + checkPointDir);
            streamingContext.checkpoint(checkPointDir);
            return streamingContext;
        };
        return Strings.isNullOrEmpty(checkPointDir) ? factory.create() :
                JavaStreamingContext.getOrCreate(checkPointDir, factory);
    }

}