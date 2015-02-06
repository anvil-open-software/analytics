package com.dematic.labs.analytics.common.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import org.junit.rules.ExternalResource;


public class KinesisStreamRule extends ExternalResource {
    public KinesisConnectorConfiguration getKinesisConnectorConfiguration() {
        return Bootstrap.getKinesisConnectorConfiguration(Bootstrap.getAWSCredentialsProvider());
    }

    public AmazonKinesisClient getAmazonKinesisClient() {
        return Bootstrap.getClient(getKinesisConnectorConfiguration());
    }

    @Override
    protected void before() throws Throwable {
        // 1) create the kinesis streams
        Bootstrap.createStreams(getAmazonKinesisClient(), getKinesisConnectorConfiguration());
    }

    @Override
    protected void after() {
        Bootstrap.destroyStream(getAmazonKinesisClient(), getKinesisConnectorConfiguration());
    }
}
