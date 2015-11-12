package com.dematic.labs.analytics.ingestion.drivers;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.dematic.labs.analytics.ingestion.sparks.drivers.EventStreamAggregator;
import com.dematic.labs.analytics.ingestion.sparks.tables.EventAggregator;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.aws.KinesisStreamRule;
import com.jayway.awaitility.Awaitility;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getStreamingContext;
import static com.dematic.labs.analytics.ingestion.sparks.drivers.EventStreamAggregator.EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME;
import static com.dematic.labs.toolkit.aws.Connections.*;
import static com.dematic.labs.toolkit.communication.EventUtils.generateEvents;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class EventStreamAggregatorTest {
    // number of events
    private static final long EVENT_COUNT = 50;
    // create a tmp dir
    private TemporaryFolder folder = new TemporaryFolder();

    // setup Kinesis
    private final KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();
    // load system properties from file and Rules
    @Rule
    public final TestRule systemPropertyRule =
            RuleChain.outerRule(new SystemPropertyRule()).around(kinesisStreamRule).around(folder);

    @Test
    public void aggregateByMinute() throws IOException {
        // start sparks driver, running in the background
        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        final String dynamoDBEndpoint = System.getProperty("dynamoDBEndpoint");
        // append user name to ensure tables are unique to person running tests to avoid collisions
        final String userNamePrefix = System.getProperty("user.name") + "_";
        final String checkpointDir = folder.getRoot().getAbsolutePath();

        // create the dynamo event table
        final String tableName = createDynamoTable(System.getProperty("dynamoDBEndpoint"), EventAggregator.class,
                userNamePrefix);
        final String leaseTable = String.format("%s%s", userNamePrefix, EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME);
        final EventStreamAggregator eventStreamAggregator = new EventStreamAggregator();

        // create the spark context
        // make Duration configurable
        final Duration pollTime = Durations.seconds(2);
        // Must add 1 more thread than the number of receivers or the output won't show properly from the driver
        final int numSparkThreads = getNumberOfShards(kinesisEndpoint, kinesisInputStream) + 1;
        final JavaStreamingContext streamingContext = getStreamingContext("local[" + numSparkThreads + "]",
                leaseTable, checkpointDir, pollTime);

        try {
            final ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.submit(() -> {
                // call the driver to consume events and store in dynamoDB
                eventStreamAggregator.aggregateEvents(
                        getJavaDStream(kinesisEndpoint, kinesisInputStream, streamingContext),
                        dynamoDBEndpoint, userNamePrefix, TimeUnit.MINUTES);
                streamingContext.start();
                streamingContext.awaitTermination();
            });

            // wait until spark is started
            Awaitility.with().pollInterval(5, TimeUnit.SECONDS).and().with().
                    pollDelay(60, TimeUnit.SECONDS).await().
                    until(() -> assertTrue(Objects.equals("ACTIVE", streamingContext.ssc().getState().toString())));

            // generate events
            kinesisStreamRule.pushEventsToKinesis(generateEvents(EVENT_COUNT, "UnitTestGenerated"));
            // set the defaults
            Awaitility.setDefaultTimeout(3, TimeUnit.MINUTES);
            // will just scan aggregate table and get count by minutes
            // poll dynamoDB table and ensure all events received
            Awaitility.with().pollInterval(10, TimeUnit.SECONDS).and().with().
                    pollDelay(10, TimeUnit.SECONDS).await().
                    until(() -> assertEquals(EVENT_COUNT, getTotalCountByAggregator(dynamoDBEndpoint, tableName)));

        } finally {
            // stop starks
            streamingContext.sparkContext().cancelAllJobs();
            streamingContext.stop(true);
            System.clearProperty("spark.master.port");
            // delete the event table
            deleteDynamoTable(getAmazonDynamoDBClient(System.getProperty("dynamoDBEndpoint")), tableName);
            // delete the lease table, always in the east
            deleteDynamoLeaseManagerTable(getAmazonDynamoDBClient(System.getProperty("dynamoDBEndpoint")),
                    leaseTable);
            // remove the checkpoint dir
            folder.delete();
        }
    }

    private static long getTotalCountByAggregator(final String dynamoDBEndpoint, final String tableName) {
        final PaginatedScanList<EventAggregator> scan = new DynamoDBMapper(
                getAmazonDynamoDBClient(dynamoDBEndpoint)).scan(EventAggregator.class, new DynamoDBScanExpression(),
                new DynamoDBMapperConfig(withTableNameReplacement(tableName)));
        final long[] count = {0};
        scan.forEach(rowByAggregate -> count[0] = rowByAggregate.getCount() + count[0]);
        return count[0];
    }
}