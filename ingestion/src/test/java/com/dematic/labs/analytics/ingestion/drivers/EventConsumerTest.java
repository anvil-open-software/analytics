package com.dematic.labs.analytics.ingestion.drivers;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.dematic.labs.analytics.ingestion.sparks.drivers.EventConsumer;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.aws.KinesisStreamRule;
import com.dematic.labs.toolkit.communication.Event;
import com.jayway.awaitility.Awaitility;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.dematic.labs.analytics.ingestion.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.ingestion.sparks.DriverUtils.getStreamingContext;
import static com.dematic.labs.toolkit.aws.Connections.deleteDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;
import static com.dematic.labs.toolkit.communication.EventTestingUtils.generateEvents;
import static org.junit.Assert.assertTrue;

public final class EventConsumerTest {
    // number of events
    private static final int EVENT_COUNT = 50;

    // setup Kinesis
    private final KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();
    // load system properties from file and Rules
    @Rule
    public final TestRule systemPropertyRule =
            RuleChain.outerRule(new SystemPropertyRule()).around(kinesisStreamRule);

    @Before
    public void createDynamoDBTable() {
        // create the table, if it does not exist
        Connections.createDynamoTable(System.getProperty("dynamoDBEndpoint"), Event.class);
    }

    @Test
    public void persistEvents() throws IOException {
        // start sparks driver, running in the background
        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        final String dynamoDBEndpoint = System.getProperty("dynamoDBEndpoint");

        final EventConsumer eventConsumer = new EventConsumer();
        // create the spark context
        final Duration pollTime = Durations.seconds(2);
        // make Duration configurable
        final JavaStreamingContext streamingContext = getStreamingContext(kinesisEndpoint,
                EventConsumer.RAW_EVENT_LEASE_TABLE_NAME, kinesisInputStream, pollTime);

        try {
            final ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.submit(() -> {
                // call the driver to consume events and store in dynamoDB
                eventConsumer.persistEvents(
                        getJavaDStream(kinesisEndpoint, kinesisInputStream, pollTime, streamingContext),
                        dynamoDBEndpoint);

                streamingContext.start();
                streamingContext.awaitTermination();
            });
            // wait until spark is started
            Awaitility.with().pollInterval(5, TimeUnit.SECONDS).and().with().
                    pollDelay(60, TimeUnit.SECONDS).await().
                    until(() -> assertTrue(Objects.equals("Started", streamingContext.ssc().state().toString())));

            System.out.println(streamingContext.sc().statusTracker());
            // generate events
            kinesisStreamRule.pushEventsToKinesis(generateEvents(EVENT_COUNT, 10, 20));
            // set the defaults
            Awaitility.setDefaultTimeout(3, TimeUnit.MINUTES);
            // poll dynamoDB table and ensure all events received
            Awaitility.with().pollInterval(10, TimeUnit.SECONDS).and().with().
                    pollDelay(10, TimeUnit.SECONDS).await().
                    until(() -> assertTrue(new DynamoDBMapper(
                            getAmazonDynamoDBClient(dynamoDBEndpoint)).scan(Event.class,
                            new DynamoDBScanExpression()).size() == EVENT_COUNT));
        } finally {
            streamingContext.sparkContext().cancelAllJobs();
            streamingContext.stop(true);
            System.clearProperty("spark.master.port");
        }
    }

    @After
    public void deleteDynamoDBTable() {
        // delete the dynamo db event table
        deleteDynamoTable(getAmazonDynamoDBClient(System.getProperty("dynamoDBEndpoint")), Event.TABLE_NAME);
    }
}