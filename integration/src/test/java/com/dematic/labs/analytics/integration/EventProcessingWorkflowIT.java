package com.dematic.labs.analytics.integration;

import com.dematic.labs.toolkit.aws.KinesisStreamRule;
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
        // ensure events were consumed by sparks
        sparksConsumedEvents();
    }

    // ensure all events are pulled from the kinesis stream, sparks is consuming the events
    private void sparksConsumedEvents() {
    }
}
