package com.dematic.labs.analytics.integration;

import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.dematic.labs.analytics.common.Event;
import com.dematic.labs.analytics.common.kinesis.KinesisStreamRule;
import com.dematic.labs.analytics.common.kinesis.consumer.EventToByteArrayTransformer;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public final class EventProcessingWorkflowIT {

    // setup Kinesis
    @Rule
    public KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();

    @Test
    public void workflow() throws IOException {
        // submit events
        produceEventsToKinesis();

    }

    private void produceEventsToKinesis() throws IOException {
        // push events to a Kinesis stream
        for (int i = 1; i <= 10; i++) {
            final PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(kinesisStreamRule.getKinesisConnectorConfiguration().KINESIS_INPUT_STREAM);
            final Event event = new Event(UUID.randomUUID(), String.format("facility_%s", i),
                    String.format("node_%s", i), DateTime.now(), DateTime.now().plusHours(1));
            putRecordRequest.setData(ByteBuffer.wrap(new EventToByteArrayTransformer().fromClass(event)));
            // partition key = which shard to send the request,
            putRecordRequest.setPartitionKey("1");
            kinesisStreamRule.getAmazonKinesisClient().putRecord(putRecordRequest);
        }
    }
}
