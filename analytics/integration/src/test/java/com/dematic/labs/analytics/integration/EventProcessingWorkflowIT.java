package com.dematic.labs.analytics.integration;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.model.*;
import com.dematic.labs.analytics.common.Event;
import com.dematic.labs.analytics.common.kinesis.KinesisStreamRule;
import com.dematic.labs.analytics.common.kinesis.consumer.EventToByteArrayTransformer;
import com.jayway.awaitility.Awaitility;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.dematic.labs.analytics.ingestion.sparks.Bootstrap.SPARKS_APP_NAME;
import static com.dematic.labs.analytics.ingestion.sparks.Bootstrap.getAWSCredentialsProvider;
import static org.junit.Assert.assertEquals;
import static samples.utils.DynamoDBUtils.deleteTable;

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
        // push events to a Kinesis stream
        for (int i = 1; i <= EVENT_COUNT; i++) {
            final PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(kinesisStreamRule.getKinesisConnectorConfiguration().KINESIS_INPUT_STREAM);
            final Event event = new Event(UUID.randomUUID(), i, i, DateTime.now(), i);
            putRecordRequest.setData(ByteBuffer.wrap(new EventToByteArrayTransformer().fromClass(event)));
            // partition key = which shard to send the request,
            putRecordRequest.setPartitionKey("1");
            kinesisStreamRule.getAmazonKinesisClient().putRecord(putRecordRequest);
        }
    }

    // ensure all events are pulled from the kinesis stream, sparks is consuming the events
    private void sparksConsumedEvents() {
        // set the defaults
        Awaitility.setDefaultTimeout(3, TimeUnit.MINUTES);
        // now poll
        Awaitility.with().pollInterval(2, TimeUnit.SECONDS).and().with().
                pollDelay(2, TimeUnit.MINUTES).await().
                until(() -> assertEquals(
                        // all events consumed from sparks, this works because we are looking for the latest records,
                        // they should be consumed by sparks
                        getRecords().size(), 0));
    }

    private List<Record> getRecords() {
        final AmazonKinesisClient kinesisClient = kinesisStreamRule.getAmazonKinesisClient();
        final KinesisConnectorConfiguration kinesisConnectorConfiguration =
                kinesisStreamRule.getKinesisConnectorConfiguration();
        // need to find the shard id, we have only setup 1 shard
        final DescribeStreamResult streamResult =
                kinesisClient.describeStream(kinesisConnectorConfiguration.KINESIS_INPUT_STREAM);
        final Shard shard = streamResult.getStreamDescription().getShards().get(0);

        final GetShardIteratorRequest shardIteratorRequest = new GetShardIteratorRequest();
        shardIteratorRequest.setStreamName(kinesisConnectorConfiguration.KINESIS_INPUT_STREAM);

        shardIteratorRequest.setShardId(shard.getShardId());
        shardIteratorRequest.setShardIteratorType(ShardIteratorType.LATEST);

        final GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(shardIteratorRequest);
        final String shardIterator = getShardIteratorResult.getShardIterator();
        final GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(shardIterator);

        final GetRecordsResult getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
        return getRecordsResult.getRecords();
    }

    @After
    public void tearDown() {
        // delete the dynamo db lease table created using spark's streaming, the lease table is always within the east region
        deleteTable(new AmazonDynamoDBClient(getAWSCredentialsProvider()), SPARKS_APP_NAME);
    }
}
