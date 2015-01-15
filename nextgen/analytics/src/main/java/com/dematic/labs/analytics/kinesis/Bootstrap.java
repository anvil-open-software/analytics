package com.dematic.labs.analytics.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import samples.utils.KinesisUtils;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;


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
    public Bootstrap(@Nonnull @KinesisClient final AmazonKinesisClient kinesisClient,
                     @Nonnull final KinesisConnectorConfiguration kinesisConnectorConfiguration) {
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
        kinesisClient.deleteStream(kinesisConnectorConfiguration.KINESIS_INPUT_STREAM);
    }
}
