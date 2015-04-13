package com.dematic.labs.analytics.common;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samples.utils.DynamoDBUtils;
import samples.utils.KinesisUtils;

import java.util.List;
import java.util.Properties;

import static com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration.*;
import static com.amazonaws.util.StringUtils.trim;

public final class AWSConnections {
    private static final Logger LOGGER = LoggerFactory.getLogger(AWSConnections.class);

    private AWSConnections() {
    }

    public static AWSCredentialsProvider getAWSCredentialsProvider() {
        // AWS credentials are set in system properties via junit.properties
        return new DefaultAWSCredentialsProviderChain();
    }

    public static AmazonKinesisClient getAmazonKinesisClient(final String awsEndpointUrl) {
        final AmazonKinesisClient kinesisClient = new AmazonKinesisClient(getAWSCredentialsProvider());
        kinesisClient.setEndpoint(awsEndpointUrl);
        return kinesisClient;
    }

    public static AmazonKinesisClient getAmazonKinesisClient(final KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        final AmazonKinesisClient client =
                new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
        client.setEndpoint(kinesisConnectorConfiguration.KINESIS_ENDPOINT);
        client.setRegion(Region.getRegion(Regions.fromName(kinesisConnectorConfiguration.REGION_NAME)));
        return client;
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

    public static void createKinesisStreams(final AmazonKinesisClient kinesisClient,
                                            final KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesisClient,
                kinesisConnectorConfiguration.KINESIS_INPUT_STREAM,
                kinesisConnectorConfiguration.KINESIS_INPUT_STREAM_SHARD_COUNT);
    }

    public static void destroyKinesisStream(final AmazonKinesisClient kinesisClient,
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

    public static AmazonDynamoDBClient getAmazonDynamoDBClient(final String awsEndpointUrl) {
        final AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(getAWSCredentialsProvider());
        dynamoDBClient.setEndpoint(awsEndpointUrl);
        return dynamoDBClient;
    }

    public static void createDynamoTable(final String awsEndpointUrl, final Class<?> clazz) {
        final AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient(awsEndpointUrl);
        final DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(dynamoDBClient);
        final CreateTableRequest createTableRequest = dynamoDBMapper.generateCreateTableRequest(clazz);
        final String tableName = createTableRequest.getTableName();
        if (tableExists(dynamoDBClient, tableName)) {
            if (tableHasCorrectSchema(dynamoDBClient, tableName, createTableRequest.getKeySchema().get(0).toString())) {
                waitForActive(dynamoDBClient, tableName);
                return;
            } else {
                throw new IllegalStateException("Table already exists and schema does not match");
            }
        }
        try {
            dynamoDBClient.createTable(createTableRequest);
        } catch (com.amazonaws.services.autoscaling.model.ResourceInUseException e) {
            throw new IllegalStateException("The table may already be getting created.", e);
        }
        LOGGER.info("Table " + tableName + " created");
        waitForActive(dynamoDBClient, tableName);
    }

    private static boolean tableExists(final AmazonDynamoDBClient dynamoDBClient, final String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        try {
            dynamoDBClient.describeTable(describeTableRequest);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    private static void waitForActive(final AmazonDynamoDBClient dynamoDBClient, final String tableName) {
        switch (getTableStatus(dynamoDBClient, tableName)) {
            case DELETING:
                throw new IllegalStateException("Table " + tableName + " is in the DELETING state");
            case ACTIVE:
                LOGGER.info("Table " + tableName + " is ACTIVE");
                return;
            default:
                long startTime = System.currentTimeMillis();
                long endTime = startTime + (10 * 60 * 1000);
                while (System.currentTimeMillis() < endTime) {
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (final InterruptedException ignore) {
                    }
                    try {
                        if (getTableStatus(dynamoDBClient, tableName) == TableStatus.ACTIVE) {
                            LOGGER.info("Table " + tableName + " is ACTIVE");
                            return;
                        }
                    } catch (final ResourceNotFoundException ignore) {
                        throw new IllegalStateException("Table " + tableName + " never went active");
                    }
                }
        }
    }

    private static TableStatus getTableStatus(final AmazonDynamoDBClient dynamoDBClient, final String tableName) {
        final DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        final DescribeTableResult describeTableResult = dynamoDBClient.describeTable(describeTableRequest);
        final String status = describeTableResult.getTable().getTableStatus();
        return TableStatus.fromValue(status);
    }

    private static boolean tableHasCorrectSchema(final AmazonDynamoDBClient dynamoDBClient, final String tableName,
                                                 final String key) {
        final DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        final DescribeTableResult describeTableResult = dynamoDBClient.describeTable(describeTableRequest);
        final TableDescription tableDescription = describeTableResult.getTable();
        if (tableDescription.getAttributeDefinitions().size() != 1) {
            LOGGER.error("The number of attribute definitions does not match the existing table.");
            return false;
        }

        final AttributeDefinition attributeDefinition = tableDescription.getAttributeDefinitions().get(0);
        if (!attributeDefinition.getAttributeName().equals(key)
                || !attributeDefinition.getAttributeType().equals(ScalarAttributeType.S.toString())) {
            LOGGER.error("Attribute name or type does not match existing table.");
            return false;
        }

        final List<KeySchemaElement> keySchemaElements = tableDescription.getKeySchema();
        if (keySchemaElements.size() != 1) {
            LOGGER.error("The number of key schema elements does not match the existing table.");
            return false;
        }

        final KeySchemaElement keySchemaElement = keySchemaElements.get(0);
        if (!keySchemaElement.getAttributeName().equals(key) ||
                !keySchemaElement.getKeyType().equals(KeyType.HASH.toString())) {
            LOGGER.error("The hash key does not match the existing table.");
            return false;
        }
        return true;
    }

    private static String getAppName() {
        return trim(System.getProperty(PROP_APP_NAME,
                String.format("%s_APP", trim(System.getProperty(PROP_KINESIS_INPUT_STREAM)))));
    }
}
