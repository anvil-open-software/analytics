package com.dematic.labs.analytics.ingestion.sparks;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.dematic.dlabs.analytics.common.Event;
import com.dematic.dlabs.analytics.common.kinesis.consumer.EventToByteArrayTransformer;
import com.dematic.dlabs.analytics.ingestion.sparks.EventProcessor;
import com.jayway.awaitility.Awaitility;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.dematic.dlabs.analytics.ingestion.sparks.Bootstrap.*;
import static org.junit.Assert.assertEquals;
import static samples.utils.DynamoDBUtils.deleteTable;
import static samples.utils.KinesisUtils.createAndWaitForStreamToBecomeAvailable;
import static samples.utils.KinesisUtils.deleteStream;

public final class EventProcessingWorkflowTest {
    private static final int EVENT_COUNT = 10;
    private static final int SHARD_COUNT = 1;
    private int shardCounter = 1;


    @Before
    public void setup() {
        // create the kinesis stream
        createAndWaitForStreamToBecomeAvailable(getAmazonKinesisClient(), assetPropertyExist(KINESIS_STREAM_KEY), SHARD_COUNT);
    }

    @Test
    public void workflow() throws IOException {
        // push events to a Kinesis stream
        final AmazonKinesisClient amazonKinesisClient = getAmazonKinesisClient();
        // push events to stream
        for (int i = 1; i <= EVENT_COUNT; i++) {
            final PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(assetPropertyExist(KINESIS_STREAM_KEY));
            final Event event = new Event(UUID.randomUUID(), String.format("facility_%s", i),
                    String.format("node_%s", i), DateTime.now(), DateTime.now().plusHours(1));
            putRecordRequest.setData(ByteBuffer.wrap(new EventToByteArrayTransformer().fromClass(event)));
            // partition key = which shard to send the request
            putRecordRequest.setPartitionKey(String.valueOf(nextShard()));
            amazonKinesisClient.putRecord(putRecordRequest);
        }
        // consume and process events from a Kinesis stream using sparks streaming
        JavaStreamingContext streamingContext = null;
        try {
            streamingContext = getStreamingContext(getConfiguration());
            final EventProcessor eventProcessor = new EventProcessor();
            eventProcessor.process(getEventStreamReceiver(streamingContext, amazonKinesisClient));
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
        deleteStream(getAmazonKinesisClient(), assetPropertyExist(KINESIS_STREAM_KEY));
        // delete the dynamo db lease table, the lease table is always within the east region
        deleteTable(new AmazonDynamoDBClient(getAWSCredentialsProvider()), SPARKS_APP_NAME);
    }
}
