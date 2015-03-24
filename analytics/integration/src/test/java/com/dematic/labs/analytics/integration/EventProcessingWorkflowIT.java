package com.dematic.labs.analytics.integration;

import com.dematic.labs.analytics.common.kinesis.KinesisStreamRule;
import org.junit.Rule;
import org.junit.Test;

public final class EventProcessingWorkflowIT {

    // setup Kinesis
    @Rule
    public KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();

    @Test
    public void workflow() {
        // submit driver to sparks
        
    }

}
