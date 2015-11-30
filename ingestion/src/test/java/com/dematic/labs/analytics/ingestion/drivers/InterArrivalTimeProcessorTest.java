package com.dematic.labs.analytics.ingestion.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.dematic.labs.analytics.common.sparks.DriverConsts;
import com.dematic.labs.analytics.ingestion.sparks.drivers.InterArrivalTimeProcessor;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTimeBucket;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.aws.KinesisStreamRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.dematic.labs.analytics.ingestion.sparks.drivers.InterArrivalTimeProcessor.INTER_ARRIVAL_TIME_LEASE_TABLE_NAME;
import static com.dematic.labs.toolkit.aws.Connections.deleteDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;

public final class InterArrivalTimeProcessorTest {
    // create a tmp dir
    private TemporaryFolder folder = new TemporaryFolder();
    // setup Kinesis
    private final KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();
    // load system properties from file and Rules
    @Rule
    public final TestRule systemPropertyRule =
            RuleChain.outerRule(new SystemPropertyRule()).around(kinesisStreamRule).around(folder);

    @Test
    public void calculateInterArrivalTimes() {
        // start sparks driver, running in the background
        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        final String dynamoDBEndpoint = System.getProperty("dynamoDBEndpoint");
        // append user name to ensure tables are unique to person running tests to avoid collisions
        final String userNamePrefix = System.getProperty("user.name") + "_";
        // set the checkpoint dir
        final String checkpointDir = folder.getRoot().getAbsolutePath();
        System.setProperty(DriverConsts.SPARK_CHECKPOINT_DIR, checkpointDir);

        try {
            final ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.submit(() -> {
                InterArrivalTimeProcessor.main(new String[]{kinesisEndpoint, kinesisInputStream, dynamoDBEndpoint,
                        userNamePrefix, "local[*]", "3"});
            });
        } finally {
            // delete dynamo tables
            final AmazonDynamoDBClient amazonDynamoDBClient = getAmazonDynamoDBClient(dynamoDBEndpoint);
            try {
                final String tableName = String.format("%s_%s", userNamePrefix, InterArrivalTimeBucket.TABLE_NAME);
                deleteDynamoTable(amazonDynamoDBClient, tableName);
            } catch (final Throwable ignore) {
            }
            try {
                final String leaseTable = String.format("%s%s", userNamePrefix, INTER_ARRIVAL_TIME_LEASE_TABLE_NAME);
                deleteDynamoTable(amazonDynamoDBClient, leaseTable);
            } catch (final Throwable ignore) {
            }
        }
    }
}
