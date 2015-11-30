package com.dematic.labs.analytics.ingestion.drivers;

import com.dematic.labs.analytics.ingestion.sparks.drivers.InterArrivalTimeProcessor;
import com.dematic.labs.analytics.ingestion.sparks.tables.EventAggregator;
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
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;

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
        final String checkpointDir = folder.getRoot().getAbsolutePath();

        // create the dynamo event table
        final String tableName = createDynamoTable(System.getProperty("dynamoDBEndpoint"), InterArrivalTimeBucket.class,
                userNamePrefix);
        final String leaseTable = String.format("%s%s", userNamePrefix, INTER_ARRIVAL_TIME_LEASE_TABLE_NAME);

        try {
            final ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.submit(() -> {
                InterArrivalTimeProcessor.main(new String[]{"", ""});
            });

        } finally {

        }
    }
}
