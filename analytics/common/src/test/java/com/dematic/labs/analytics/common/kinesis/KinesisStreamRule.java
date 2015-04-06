package com.dematic.labs.analytics.common.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.dematic.labs.analytics.common.AWSConnections;
import org.junit.rules.ExternalResource;


public final class KinesisStreamRule extends ExternalResource {
    public KinesisConnectorConfiguration getKinesisConnectorConfiguration() {
        return AWSConnections.getKinesisConnectorConfiguration(AWSConnections.getAWSCredentialsProvider());
    }

    public AmazonKinesisClient getAmazonKinesisClient() {
        return AWSConnections.getAmazonKinesisClient(getKinesisConnectorConfiguration());
    }

    @Override
    protected void before() throws Throwable {
        // 1) create the kinesis streams
        AWSConnections.createKinesisStreams(getAmazonKinesisClient(), getKinesisConnectorConfiguration());
    }

    @Override
    protected void after() {
        AWSConnections.destroyKinesisStream(getAmazonKinesisClient(), getKinesisConnectorConfiguration());
    }
}
