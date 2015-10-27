package com.dematic.labs.analytics.ingestion.drivers;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.dematic.labs.analytics.common.sparks.DematicSparkSession;
import com.dematic.labs.analytics.common.sparks.DriverUtils;
import com.dematic.labs.analytics.ingestion.sparks.drivers.EventStreamAggregator;
import com.dematic.labs.analytics.ingestion.sparks.tables.EventAggregator;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.aws.KinesisStreamRule;
import com.jayway.awaitility.Awaitility;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getStreamingContext;
import static com.dematic.labs.analytics.ingestion.sparks.drivers.EventStreamAggregator.EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.deleteDynamoLeaseManagerTable;
import static com.dematic.labs.toolkit.aws.Connections.deleteDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;
import static com.dematic.labs.toolkit.communication.EventUtils.generateEvents;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class DematicSessionTest {

    @Test
    public void test() {
        String[] args = {"https://kinesis.$DYNAMODB_AWS_REGION.amazonaws.com",
                "test_stream",
                "https://dynamodb.$DYNAMODB_AWS_REGION.amazonaws.com",
                "test_",
                "5",
                "MINUTES"};

        DematicSparkSession session = new DematicSparkSession(EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME, args);
        session.setCheckPointDirectoryFromSystemProperties(false);
        System.setProperty("spark.checkpoint.dir","/tmp");
        session.setCheckPointDirectoryFromSystemProperties(true);
    }

}