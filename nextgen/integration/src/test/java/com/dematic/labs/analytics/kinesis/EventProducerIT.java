package com.dematic.labs.analytics.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.dematic.labs.analytics.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.joda.time.DateTime;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.UUID;

import static com.dematic.labs.analytics.kinesis.ClientProducer.*;

public final class EventProducerIT {
    @Test
    public void pushEvents() throws JsonProcessingException {
        final KinesisConnectorConfiguration kinesisConnectorConfiguration =
                getKinesisConnectorConfiguration(getAWSCredentialsProvider());
        final AmazonKinesisClient client = getClient(kinesisConnectorConfiguration);

        // push events to stream
        for (int i = 1; i <= 10; i++) {
            final PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(kinesisConnectorConfiguration.KINESIS_INPUT_STREAM);
            final Event event = new Event(UUID.randomUUID(), String.format("facility_%s", i),
                    String.format("node_%s", i), DateTime.now(), DateTime.now().plusHours(1));
            putRecordRequest.setData(ByteBuffer.wrap(event.toJson().getBytes(Charset.defaultCharset())));
            putRecordRequest.setPartitionKey(String.valueOf(kinesisConnectorConfiguration.KINESIS_INPUT_STREAM_SHARD_COUNT));
            client.putRecord(putRecordRequest);
        }
        // todo: assert events where inserted
    }
}
