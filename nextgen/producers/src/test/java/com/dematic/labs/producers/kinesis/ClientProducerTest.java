package com.dematic.labs.producers.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import org.junit.Assert;
import org.junit.Test;

import static com.dematic.labs.producers.kinesis.ClientProducer.*;

public class ClientProducerTest {
    @Test
    public void createKinesisClient() {
        final AmazonKinesisClient client = getClient(getKinesisConnectorConfiguration(getAWSCredentialsProvider()));
        Assert.assertNotNull(client);
    }
}
