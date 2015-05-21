package com.dematic.labs.analytics.ingestion.drivers;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.dematic.labs.analytics.ingestion.sparks.drivers.EventConsumer;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.aws.KinesisStreamRule;
import com.dematic.labs.toolkit.communication.Event;
import com.jayway.awaitility.Awaitility;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.dematic.labs.toolkit.aws.Connections.*;
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

    @Test
    public void persistEvents() throws IOException {
        // start sparks driver, running in the background
        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        final String dynamoDBEndpoint = System.getProperty("dynamoDBEndpoint");

        final ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(() -> {
            // call the driver to consume events and store in dynamoDB
            final String[] driverProperties = {kinesisEndpoint, kinesisInputStream, dynamoDBEndpoint};
            EventConsumer.main(driverProperties);
        });

        // ensure dynamo table exist, gets created by the driver
        // set the defaults
        Awaitility.setDefaultTimeout(3, TimeUnit.MINUTES);
        // now poll
        Awaitility.with().pollInterval(2, TimeUnit.SECONDS).and().with().
                pollDelay(1, TimeUnit.MINUTES).await().
                until(() -> assertTrue(
                        TableStatus.ACTIVE.name().equals(getTableStatus(getAmazonDynamoDBClient(dynamoDBEndpoint),
                                Event.TABLE_NAME).name())));

        // generate events
        kinesisStreamRule.pushEventsToKinesis(generateEvents(EVENT_COUNT, 10, 20));
        // poll dynamoDB table and ensure all events received
        Awaitility.with().pollInterval(10, TimeUnit.SECONDS).and().with().
                pollDelay(30, TimeUnit.SECONDS).await().
                until(() -> assertTrue(new DynamoDBMapper(
                        getAmazonDynamoDBClient(dynamoDBEndpoint)).scan(Event.class,
                        new DynamoDBScanExpression()).size() == EVENT_COUNT));
    }

    @After
    public void tearDown() {
        // delete the dynamo db event table
        deleteDynamoTable(getAmazonDynamoDBClient(System.getProperty("dynamoDBEndpoint")), Event.TABLE_NAME);
    }
}