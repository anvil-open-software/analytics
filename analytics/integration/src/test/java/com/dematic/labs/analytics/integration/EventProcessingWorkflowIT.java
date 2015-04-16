package com.dematic.labs.analytics.integration;

import com.dematic.labs.analytics.common.kinesis.KinesisStreamRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

@Ignore // place holder
public final class EventProcessingWorkflowIT {
    private final static int EVENT_COUNT = 10;

    // setup Kinesis
    @Rule
    public KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();

    @Test
    public void workflow() throws IOException {
        // submit events
        produceEventsToKinesis();
        // ensure events were consumed by sparks
        sparksConsumedEvents();
    }

    private void produceEventsToKinesis() throws IOException {
        kinesisStreamRule.generateEventsAndPush(EVENT_COUNT);
    }

    // ensure all events are pulled from the kinesis stream, sparks is consuming the events
    private void sparksConsumedEvents() {
    }
}
