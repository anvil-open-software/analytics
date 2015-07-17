package com.dematic.labs.analytics.store.drivers;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.dematic.labs.analytics.common.sparks.DriverUtils;
import com.dematic.labs.analytics.store.sparks.drivers.Persister;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.aws.KinesisStreamRule;
import com.dematic.labs.toolkit.communication.Event;
import com.jayway.awaitility.Awaitility;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getStreamingContext;
import static com.dematic.labs.analytics.store.sparks.drivers.Persister.RAW_EVENT_LEASE_TABLE_NAME;
import static com.dematic.labs.toolkit.aws.Connections.*;
import static com.dematic.labs.toolkit.communication.EventUtils.generateEvents;
import static org.junit.Assert.assertTrue;

public final class PersisterTest {
    // number of events
    private static final int EVENT_COUNT = 50;

    // setup Kinesis
    private final KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();
    // load system properties from file and Rules
    @Rule
    public final TestRule systemPropertyRule =
            RuleChain.outerRule(new SystemPropertyRule()).around(kinesisStreamRule);

    @Test
    public void persistEvents() throws IOException {
        // start sparks driver, running in the background
        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        final String dynamoDBEndpoint = System.getProperty("dynamoDBEndpoint");
        // append user name to ensure tables are unique to person running tests to avoid collisions
        final String userNamePrefix = System.getProperty("user.name") + "_";

        // create the dynamo event table
        final String tableName = createDynamoTable(System.getProperty("dynamoDBEndpoint"), Event.class, userNamePrefix);
        final String leaseTable = String.format("%s%s", userNamePrefix, RAW_EVENT_LEASE_TABLE_NAME);

        final Persister persister = new Persister();
        // create the spark context
        // make Duration configurable
        final Duration pollTime = Durations.seconds(2);
        // Must add 1 more thread than the number of receivers or the output won't show properly from the driver
        final int numSparkThreads = DriverUtils.getNumberOfShards(kinesisEndpoint, kinesisInputStream) + 1;
        final JavaStreamingContext streamingContext = getStreamingContext("local[" + numSparkThreads + "]",
                leaseTable, null, pollTime);

        try {
            final ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.submit(() -> {
                // call the driver to consume events and store in dynamoDB
                persister.persistEvents(
                        getJavaDStream(kinesisEndpoint, kinesisInputStream, pollTime, streamingContext),
                        dynamoDBEndpoint, userNamePrefix);
                streamingContext.start();
                streamingContext.awaitTermination();
            });

            // wait until spark is started
            Awaitility.with().pollInterval(5, TimeUnit.SECONDS).and().with().
                    pollDelay(60, TimeUnit.SECONDS).await().
                    until(() -> assertTrue(Objects.equals("ACTIVE", streamingContext.ssc().getState().toString())));

            // generate events
            kinesisStreamRule.pushEventsToKinesis(generateEvents(EVENT_COUNT, 10, 20));
            // set the defaults
            Awaitility.setDefaultTimeout(3, TimeUnit.MINUTES);
            // poll dynamoDB table and ensure all events received
            Awaitility.with().pollInterval(10, TimeUnit.SECONDS).and().with().
                    pollDelay(10, TimeUnit.SECONDS).await().
                    until(() -> assertTrue(new DynamoDBMapper(
                            getAmazonDynamoDBClient(dynamoDBEndpoint)).scan(Event.class,
                            new DynamoDBScanExpression(),
                            new DynamoDBMapperConfig(withTableNameReplacement(tableName))).size() == EVENT_COUNT));
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
        }
    }
}