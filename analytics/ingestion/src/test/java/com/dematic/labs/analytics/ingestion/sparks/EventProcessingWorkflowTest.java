package com.dematic.labs.analytics.ingestion.sparks;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.dematic.labs.analytics.common.Event;
import com.dematic.labs.analytics.common.kinesis.KinesisStreamRule;
import com.dematic.labs.analytics.common.kinesis.consumer.EventToByteArrayTransformer;
import com.jayway.awaitility.Awaitility;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.dematic.labs.analytics.ingestion.sparks.Bootstrap.*;
import static org.junit.Assert.assertEquals;
import static samples.utils.DynamoDBUtils.deleteTable;

// think about removing this test
@Ignore
public final class EventProcessingWorkflowTest {
    private static final int EVENT_COUNT = 10;
    private static final int SHARD_COUNT = 1;
    private int shardCounter = 1;

    @Rule
    public KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();


    //@Test
    public void workflow() throws IOException {
        // push events to a Kinesis stream
        for (int i = 1; i <= EVENT_COUNT; i++) {
            final PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(kinesisStreamRule.getKinesisConnectorConfiguration().KINESIS_INPUT_STREAM);
            final Event event = new Event(UUID.randomUUID(), i, i, DateTime.now(), i);
            putRecordRequest.setData(ByteBuffer.wrap(new EventToByteArrayTransformer().fromClass(event)));
            // partition key = which shard to send the request,
            putRecordRequest.setPartitionKey(String.valueOf(nextShard()));
            kinesisStreamRule.getAmazonKinesisClient().putRecord(putRecordRequest);
        }
        // consume and process events from a Kinesis stream using sparks streaming
        JavaStreamingContext streamingContext = null;
        try {
            streamingContext = getStreamingContext(getLocalConfiguration(), Durations.seconds(2));
            final EventProcessor eventProcessor = new EventProcessor();
            eventProcessor.process(getEventStreamReceiver(streamingContext, kinesisStreamRule.getAmazonKinesisClient(),
                    kinesisStreamRule.getKinesisConnectorConfiguration()));
            streamingContext.start();
            // wait for all reads/processing to complete
            Awaitility.await().atMost(5, TimeUnit.MINUTES).until(() ->
                    assertEquals(eventProcessor.getEventsProcessed(), EVENT_COUNT));
        } finally {
            if (streamingContext != null) {
                streamingContext.stop();
            }
        }
    }

    private int nextShard() {
        if (shardCounter == SHARD_COUNT) {
            shardCounter = 1;
            return SHARD_COUNT;
        }
        //noinspection ValueOfIncrementOrDecrementUsed
        return shardCounter++;
    }

    @After
    public void tearDown() {
        // delete the dynamo db lease table created using spark's streaming, the lease table is always within the east region
        deleteTable(new AmazonDynamoDBClient(getAWSCredentialsProvider()), SPARKS_APP_NAME);
    }
}
