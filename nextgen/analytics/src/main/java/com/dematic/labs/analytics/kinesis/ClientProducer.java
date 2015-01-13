package com.dematic.labs.analytics.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

import javax.annotation.Nonnull;
import javax.enterprise.inject.Produces;
import java.util.Properties;

import static com.amazonaws.util.StringUtils.trim;
import static com.dematic.labs.analytics.kinesis.KinesisConnectorConfiguration.*;

public final class ClientProducer {
    @Produces
    public static AWSCredentialsProvider getAWSCredentialsProvider() {
        // AWS credentials are set in system properties via junit.properties
        return new DefaultAWSCredentialsProviderChain();
    }

    @Produces
    public static KinesisConnectorConfiguration getKinesisConnectorConfiguration(@SuppressWarnings("CdiInjectionPointsInspection") @Nonnull final AWSCredentialsProvider credentialsProvider) {
        final Properties properties = new Properties();
        properties.setProperty(PROP_KINESIS_ENDPOINT, trim(System.getProperty(PROP_KINESIS_ENDPOINT)));
        properties.setProperty(PROP_REGION_NAME, trim(System.getProperty(PROP_REGION_NAME)));
        properties.setProperty(PROP_KINESIS_INPUT_STREAM, trim(System.getProperty(PROP_KINESIS_INPUT_STREAM)));
        return new KinesisConnectorConfiguration(properties, credentialsProvider);
    }

    @Produces
    @KinesisClient
    public static AmazonKinesisClient getClient(@Nonnull final KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        final AmazonKinesisClient client =
                new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
        client.setEndpoint(kinesisConnectorConfiguration.KINESIS_ENDPOINT);
        client.setRegion(Region.getRegion(Regions.fromName(kinesisConnectorConfiguration.REGION_NAME)));
        return client;
    }
}
