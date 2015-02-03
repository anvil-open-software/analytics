package com.dematic.labs.producers.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;

import javax.enterprise.inject.Produces;
import java.util.Properties;

import static com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration.*;
import static com.amazonaws.util.StringUtils.trim;

public final class ClientProducer {
    @Produces
    public static AWSCredentialsProvider getAWSCredentialsProvider() {
        // AWS credentials are set in system properties via junit.properties
        return new DefaultAWSCredentialsProviderChain();
    }

    @Produces
    public static KinesisConnectorConfiguration getKinesisConnectorConfiguration(@SuppressWarnings("CdiInjectionPointsInspection") final AWSCredentialsProvider credentialsProvider) {
        final Properties properties = new Properties();
        properties.setProperty(PROP_KINESIS_ENDPOINT, trim(System.getProperty(PROP_KINESIS_ENDPOINT)));
        properties.setProperty(PROP_REGION_NAME, trim(System.getProperty(PROP_REGION_NAME)));
        properties.setProperty(PROP_KINESIS_INPUT_STREAM, trim(System.getProperty(PROP_KINESIS_INPUT_STREAM)));
        properties.setProperty(PROP_APP_NAME, getAppName());
        // change for production, set to 10 for testing
        properties.setProperty(PROP_BUFFER_RECORD_COUNT_LIMIT, "10");
        return new KinesisConnectorConfiguration(properties, credentialsProvider);
    }

    @Produces
    @KinesisClient
    public static AmazonKinesisClient getClient(final KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        final AmazonKinesisClient client =
                new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
        client.setEndpoint(kinesisConnectorConfiguration.KINESIS_ENDPOINT);
        client.setRegion(Region.getRegion(Regions.fromName(kinesisConnectorConfiguration.REGION_NAME)));
        return client;
    }

    private static String getAppName() {
        // default is the stream name plus timestamp
        return trim(System.getProperty(PROP_APP_NAME,
                String.format("%s_APP_%s", trim(System.getProperty(PROP_KINESIS_INPUT_STREAM)),
                        System.currentTimeMillis())));
    }
}
