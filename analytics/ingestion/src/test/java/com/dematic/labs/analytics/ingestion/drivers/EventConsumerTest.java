package com.dematic.labs.analytics.ingestion.drivers;

import com.dematic.labs.analytics.common.SystemPropertyRule;
import com.dematic.labs.analytics.common.kinesis.KinesisStreamRule;
import com.dematic.labs.analytics.ingestion.sparks.drivers.EventConsumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

public final class EventConsumerTest {
    private static final int EVENT_COUNT = 50;

    // setup Kinesis
    private final KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();
    // load system properties from file and Rules
    @Rule
    public final TestRule systemPropertyRule =
            RuleChain.outerRule(new SystemPropertyRule()).around(kinesisStreamRule);


    @Test
    public void persistEvents() throws IOException {
        // generate events
        kinesisStreamRule.generateEventsAndPush(EVENT_COUNT);
        // call the driver to consume events and store in dynamoDB
        final String[] driverProperties = {
                System.getProperty("kinesisEndpoint"),
                System.getProperty("kinesisInputStream"),
                System.getProperty("dynamoDBEndpoint")};
        EventConsumer.main(driverProperties);
    }
}