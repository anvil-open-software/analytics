package com.dematic.labs.analytics.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import org.junit.Assert;
import org.junit.Test;

import static com.dematic.labs.analytics.kinesis.ClientProducer.*;

public class ClientProducerTest {
    @Test
    public void createKinesisClient() {
        final AmazonKinesisClient client = getClient(getKinesisConnectorConfiguration(getAWSCredentialsProvider()));
        Assert.assertNotNull(client);
    }
}
