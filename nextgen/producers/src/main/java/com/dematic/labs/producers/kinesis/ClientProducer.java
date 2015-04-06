package com.dematic.labs.producers.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.dematic.labs.analytics.common.AWSConnections;

import javax.enterprise.inject.Produces;

public final class ClientProducer {
    @Produces
    public static AWSCredentialsProvider getAWSCredentialsProvider() {
        return AWSConnections.getAWSCredentialsProvider();
    }

    @Produces
    public static KinesisConnectorConfiguration getKinesisConnectorConfiguration(@SuppressWarnings("CdiInjectionPointsInspection") final AWSCredentialsProvider credentialsProvider) {
        return AWSConnections.getKinesisConnectorConfiguration(credentialsProvider);
    }

    @Produces
    public static AmazonKinesisClient getClient(final KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        return AWSConnections.getAmazonKinesisClient(kinesisConnectorConfiguration);
    }
}
