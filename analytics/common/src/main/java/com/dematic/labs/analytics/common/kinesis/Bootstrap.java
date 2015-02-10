package com.dematic.labs.analytics.common.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import samples.utils.DynamoDBUtils;
import samples.utils.KinesisUtils;

import java.util.Properties;

import static com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration.*;
import static com.amazonaws.util.StringUtils.trim;

public final class Bootstrap {
    private Bootstrap() {
    }

    public static AWSCredentialsProvider getAWSCredentialsProvider() {
        // AWS credentials are set in system properties via junit.properties
        return new DefaultAWSCredentialsProviderChain();
    }

    public static KinesisConnectorConfiguration getKinesisConnectorConfiguration(final AWSCredentialsProvider credentialsProvider) {
        final Properties properties = new Properties();
        properties.setProperty(PROP_KINESIS_ENDPOINT, trim(System.getProperty(PROP_KINESIS_ENDPOINT)));
        properties.setProperty(PROP_REGION_NAME, trim(System.getProperty(PROP_REGION_NAME)));
        properties.setProperty(PROP_KINESIS_INPUT_STREAM, trim(System.getProperty(PROP_KINESIS_INPUT_STREAM)));
        properties.setProperty(PROP_APP_NAME, getAppName());
        // change for production, set to 10 for testing
        properties.setProperty(PROP_BUFFER_RECORD_COUNT_LIMIT, "10");
        return new KinesisConnectorConfiguration(properties, credentialsProvider);
    }

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
                String.format("%s_APP", trim(System.getProperty(PROP_KINESIS_INPUT_STREAM)))));
    }

    public static void createStreams(final AmazonKinesisClient kinesisClient,
                                     final KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesisClient,
                kinesisConnectorConfiguration.KINESIS_INPUT_STREAM,
                kinesisConnectorConfiguration.KINESIS_INPUT_STREAM_SHARD_COUNT);
    }

    public static void destroyStream(final AmazonKinesisClient kinesisClient,
                                     final KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        try {
            // delete the stream
            kinesisClient.deleteStream(kinesisConnectorConfiguration.KINESIS_INPUT_STREAM);
        } finally {
            // delete the dynamo lease mgr table
            final AmazonDynamoDBClient amazonDynamoDBClient =
                    new AmazonDynamoDBClient(kinesisConnectorConfiguration.AWS_CREDENTIALS_PROVIDER);
            amazonDynamoDBClient.setRegion(Region.getRegion(Regions.fromName(kinesisConnectorConfiguration.REGION_NAME)));
            DynamoDBUtils.deleteTable(amazonDynamoDBClient,
                    kinesisConnectorConfiguration.APP_NAME);
        }
    }
}
