package com.dematic.labs.producers.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.dematic.labs.analytics.common.Event;
import com.dematic.labs.analytics.common.kinesis.KinesisStreamRule;
import com.dematic.labs.analytics.common.kinesis.consumer.EventEmitter;
import com.dematic.labs.analytics.common.kinesis.consumer.EventPipeline;
import com.dematic.labs.analytics.common.kinesis.consumer.EventToByteArrayTransformer;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

public final class EventProducerConsumerIT {
    @Rule
    public KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();

    @Test
    public void pushAndConsumeEvents() throws IOException, InterruptedException {
        final KinesisConnectorConfiguration kinesisConnectorConfiguration =
                kinesisStreamRule.getKinesisConnectorConfiguration();
        final AmazonKinesisClient client = kinesisStreamRule.getAmazonKinesisClient();

        // push events to stream
        for (int i = 1; i <= 10; i++) {
            final PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(kinesisConnectorConfiguration.KINESIS_INPUT_STREAM);
            final Event event = new Event(UUID.randomUUID(), String.format("facility_%s", i),
                    String.format("node_%s", i), DateTime.now(), DateTime.now().plusHours(1));
            putRecordRequest.setData(ByteBuffer.wrap(new EventToByteArrayTransformer().fromClass(event)));
            putRecordRequest.setPartitionKey(String.valueOf(kinesisConnectorConfiguration.KINESIS_INPUT_STREAM_SHARD_COUNT));
            client.putRecord(putRecordRequest);
        }

        // consume records from stream
        // for now we are just creating a consumer in a unit tests, this will eventually be in the application
        final class EventExecutor extends KinesisConnectorExecutorBase<Event, byte[]> {
            private final EventPipeline eventPipeline = new EventPipeline(new EventEmitter());

            public EventExecutor() {
                initialize(kinesisConnectorConfiguration);
            }

            @Override
            public KinesisConnectorRecordProcessorFactory<Event, byte[]> getKinesisConnectorRecordProcessorFactory() {
                return new KinesisConnectorRecordProcessorFactory<>(eventPipeline, kinesisConnectorConfiguration);
            }

            public void shutdown() {
                worker.shutdown();
            }

            public int getEventEmitterCount() {
                return ((EventEmitter) eventPipeline.getEmitter(kinesisConnectorConfiguration)).getConsumedEventCount();
            }
        }

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final EventExecutor eventConsumer = new EventExecutor();
        executorService.execute(eventConsumer);

        // assert that we read 10 events
        await().atMost(10, TimeUnit.MINUTES).until(() ->
                assertEquals(eventConsumer.getEventEmitterCount(), 10));
        eventConsumer.shutdown();
    }
}
