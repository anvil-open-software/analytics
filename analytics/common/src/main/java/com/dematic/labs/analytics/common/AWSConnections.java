package com.dematic.labs.analytics.common;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samples.utils.KinesisUtils;

import static samples.utils.DynamoDBUtils.deleteTable;

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

    public static void createKinesisStreams(final AmazonKinesisClient kinesisClient, final String kinesisStream,
                                            final int kinesisStreamShardCount) {
        KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesisClient, kinesisStream, kinesisStreamShardCount);
    }
    public static boolean kinesisStreamsExist(final AmazonKinesisClient kinesisClient, final String kinesisStream) {
        try {
            kinesisClient.describeStream(kinesisStream);
            return true;
        } catch (final Throwable ignore) {
           return false;
        }
    }

    public static void deleteKinesisStream(final AmazonKinesisClient kinesisClient, final String kinesisStream) {
        kinesisClient.deleteStream(kinesisStream);
    }

    public static void deleteDynamoLeaseManagerTable(final AmazonDynamoDBClient dynamoDBClient, final String tableName) {
        deleteTable(dynamoDBClient, tableName);
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
        if (dynamoTableExists(dynamoDBClient, tableName)) {
            waitForActive(dynamoDBClient, tableName);
            return;
        }
        try {
            // just using default read/write provisioning, will need to use a service to monitor and scale accordingly
            createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(10L, 10L));
            dynamoDBClient.createTable(createTableRequest);
        } catch (com.amazonaws.services.autoscaling.model.ResourceInUseException e) {
            throw new IllegalStateException("The table may already be getting created.", e);
        }
        LOGGER.info("Table " + tableName + " created");
        waitForActive(dynamoDBClient, tableName);
    }

    public static boolean dynamoTableExists(final AmazonDynamoDBClient dynamoDBClient, final String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        try {
            dynamoDBClient.describeTable(describeTableRequest);
            return true;
        } catch (Throwable ignore) {
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

    public static TableStatus getTableStatus(final AmazonDynamoDBClient dynamoDBClient, final String tableName) {
        final DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        final DescribeTableResult describeTableResult = dynamoDBClient.describeTable(describeTableRequest);
        final String status = describeTableResult.getTable().getTableStatus();
        return TableStatus.fromValue(status);
    }
}
