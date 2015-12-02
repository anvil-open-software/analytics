package com.dematic.labs.analytics.ingestion.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.dematic.labs.analytics.common.sparks.DriverConsts;
import com.dematic.labs.analytics.ingestion.sparks.drivers.InterArrivalTimeProcessor;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTime;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.aws.KinesisStreamRule;
import com.dematic.labs.toolkit.simulators.NodeExecutor;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.dematic.labs.analytics.ingestion.sparks.drivers.InterArrivalTimeProcessor.INTER_ARRIVAL_TIME_LEASE_TABLE_NAME;
import static com.dematic.labs.toolkit.aws.Connections.deleteDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;

@Ignore
public final class InterArrivalTimeProcessorTest {
    private static final String AVG_INTER_ARRIVAL_TIME = "10";
    // create a tmp dir
    private TemporaryFolder folder = new TemporaryFolder();
    // setup Kinesis
    private final KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();
    // load system properties from file and Rules
    @Rule
    public final TestRule systemPropertyRule =
            RuleChain.outerRule(new SystemPropertyRule()).around(kinesisStreamRule).around(folder);

    //@Test
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

        // start sparks in a separate thread
        try {
            final ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.submit(() -> InterArrivalTimeProcessor.main(new String[]{kinesisEndpoint,
                    kinesisInputStream, dynamoDBEndpoint, userNamePrefix, "local[*]", "3", AVG_INTER_ARRIVAL_TIME}));
            // give spark some time to start
            TimeUnit.SECONDS.sleep(20);
            // push events to kinesis stream using the node event generator
            NodeExecutor.main(new String[]{"1", "20", "15", AVG_INTER_ARRIVAL_TIME, "2", kinesisEndpoint, kinesisInputStream,
                    InterArrivalTimeProcessorTest.class.getSimpleName()});
            // todo: figure out how to run to success
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(20, TimeUnit.MINUTES);
        } catch (final InterruptedException ignore) {
        } finally {
            // delete dynamo tables
            final AmazonDynamoDBClient amazonDynamoDBClient = getAmazonDynamoDBClient(dynamoDBEndpoint);
            try {
                final String tableName = String.format("%s_%s", userNamePrefix, InterArrivalTime.TABLE_NAME);
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
