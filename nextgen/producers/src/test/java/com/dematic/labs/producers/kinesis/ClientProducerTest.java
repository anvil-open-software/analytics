package com.dematic.labs.producers.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.junit.Assert;
import org.junit.Test;

import static com.dematic.labs.producers.kinesis.ClientProducer.getAWSCredentialsProvider;

public class ClientProducerTest {
    @Test
    public void createKinesisClient() {
        final AWSCredentialsProvider credentialsProvider = getAWSCredentialsProvider();
        Assert.assertNotNull(credentialsProvider);
    }
}
