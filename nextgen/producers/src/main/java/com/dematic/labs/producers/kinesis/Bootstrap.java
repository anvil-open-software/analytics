package com.dematic.labs.producers.kinesis;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import samples.utils.DynamoDBUtils;
import samples.utils.KinesisUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;


@SuppressWarnings("UnusedDeclaration")
@Startup
@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class Bootstrap {
    private AmazonKinesisClient kinesisClient;
    private KinesisConnectorConfiguration kinesisConnectorConfiguration;

    /* needed by weld */
    @SuppressWarnings("UnusedDeclaration")
    public Bootstrap() {
    }

    @Inject
    public Bootstrap(@KinesisClient final AmazonKinesisClient kinesisClient,
                     final KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        this.kinesisClient = kinesisClient;
        this.kinesisConnectorConfiguration = kinesisConnectorConfiguration;
    }

    @PostConstruct
    public void createStreams() {
        KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesisClient,
                kinesisConnectorConfiguration.KINESIS_INPUT_STREAM,
                kinesisConnectorConfiguration.KINESIS_INPUT_STREAM_SHARD_COUNT);
    }

    @PreDestroy
    public void destroyStream() {
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
