package com.dematic.labs.analytics.common.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.dematic.labs.analytics.common.AWSConnections;
import com.dematic.labs.analytics.common.Event;
import com.dematic.labs.analytics.common.kinesis.consumer.EventToByteArrayTransformer;
import org.joda.time.DateTime;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;


public final class KinesisStreamRule extends ExternalResource {
    @Override
    protected void before() throws Throwable {
        // 1) create the kinesis streams
        AWSConnections.createKinesisStreams(getAmazonKinesisClient(), getKinesisConnectorConfiguration());
    }

    @Override
    protected void after() {
        AWSConnections.destroyKinesisStream(getAmazonKinesisClient(), getKinesisConnectorConfiguration());
    }

    public KinesisConnectorConfiguration getKinesisConnectorConfiguration() {
        return AWSConnections.getKinesisConnectorConfiguration(AWSConnections.getAWSCredentialsProvider());
    }

    public AmazonKinesisClient getAmazonKinesisClient() {
        return AWSConnections.getAmazonKinesisClient(getKinesisConnectorConfiguration());
    }

    public void generateEventsAndPush(final int count) throws IOException {
        // push events to a Kinesis stream
        for (int i = 1; i <= count; i++) {
            final PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(getKinesisConnectorConfiguration().KINESIS_INPUT_STREAM);
            final Event event = new Event(UUID.randomUUID(), i, i, DateTime.now(), i);
            putRecordRequest.setData(ByteBuffer.wrap(new EventToByteArrayTransformer().fromClass(event)));
            // partition key = which shard to send the request,
            putRecordRequest.setPartitionKey("1");
            getAmazonKinesisClient().putRecord(putRecordRequest);
        }
    }
}
