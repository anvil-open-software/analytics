package com.dematic.labs.analytics.ingestion.drivers.stateful;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.dematic.labs.analytics.common.spark.DriverConsts;
import com.dematic.labs.analytics.ingestion.sparks.drivers.stateful.CycleTimeProcessor;
import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.aws.KinesisStreamRule;
import com.jayway.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.toolkit.aws.Connections.deleteDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;
import static com.dematic.labs.toolkit.communication.EventUtils.generateCycleTimeEvents;
import static org.junit.Assert.assertEquals;

public final class CycleTimeProcessorTest {

    private static final String NODE_ID = "node_1";
    private static final UUID JOB_ID = UUID.randomUUID();

    // create a tmp dir
    private TemporaryFolder folder = new TemporaryFolder();
    // setup Kinesis
    private final KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();
    // load system properties from file and Rules
    @Rule
    public final TestRule systemPropertyRule =
            RuleChain.outerRule(new SystemPropertyRule()).around(kinesisStreamRule).around(folder);

    @Test
    public void calculateCycleTimes() throws InterruptedException {
        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        final String dynamoDBEndpoint = System.getProperty("dynamoDBEndpoint");
        // append user name to ensure tables are unique to person running tests to avoid collisions
        final String userNamePrefix = System.getProperty("user.name") + "_";
        // set the checkpoint dir
        final String checkpointDir = folder.getRoot().getAbsolutePath();
        System.setProperty(DriverConsts.SPARK_CHECKPOINT_DIR, checkpointDir);
        System.setProperty("spark.driver.allowMultipleContexts", "true"); //todo: need to figure out multiple context
        // start sparks in a separate thread
        try {
            final ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.submit(() -> CycleTimeProcessor.main(new String[]{kinesisEndpoint, kinesisInputStream,
                    dynamoDBEndpoint, userNamePrefix, "local[*]", "10", "5", "10"}));

            // give spark some time to start
            TimeUnit.SECONDS.sleep(20);

            // ensure all use-cases succeed
            checkJobCount(dynamoDBEndpoint, userNamePrefix);
        } finally {
            // delete dynamo tables
            final AmazonDynamoDBClient amazonDynamoDBClient = getAmazonDynamoDBClient(dynamoDBEndpoint);
            try {
                final String tableName = String.format("%s%s", userNamePrefix, CycleTime.TABLE_NAME);
                deleteDynamoTable(amazonDynamoDBClient, tableName);
            } catch (final Throwable ignore) {
            }
            try {
                final String leaseTable = String.format("%s%s", userNamePrefix,
                        CycleTimeProcessor.CYCLE_TIME_PROCESSOR_LEASE_TABLE_NAME);
                deleteDynamoTable(amazonDynamoDBClient, leaseTable);
            } catch (final Throwable ignore) {
            }
        }
    }

    private void checkJobCount(final String dynamoDBEndpoint, final String tablePrefix) {
        // push job pair to kinesis
        kinesisStreamRule.pushEventsToKinesis(generateCycleTimeEvents(2, NODE_ID, JOB_ID, 5));


        // set the defaults timeouts
        Awaitility.setDefaultTimeout(3, TimeUnit.MINUTES);

        final DynamoDBMapper dynamoDBMapper = getDynamoDBMapper(dynamoDBEndpoint, tablePrefix);
        // poll dynamoDB CT table and ensure job count == 1
        Awaitility.with().pollInterval(10, TimeUnit.SECONDS).and().with().
                pollDelay(10, TimeUnit.SECONDS).await().
                until(() -> assertEquals(1, getJobCount(NODE_ID, dynamoDBMapper)));
    }

    private static DynamoDBMapper getDynamoDBMapper(final String dynamoDBEndpoint, final String tablePrefix) {
        final AmazonDynamoDBClient dynamoDBClient = Connections.getAmazonDynamoDBClient(dynamoDBEndpoint);
        return new DynamoDBMapper(dynamoDBClient, new DynamoDBMapperConfig(withTableNamePrefix(tablePrefix)));
    }

    private static long getJobCount(final String nodeId, final DynamoDBMapper dynamoDBMapper) {
        final CycleTime cycleTime = findCycleTimeByNodeId(nodeId, dynamoDBMapper);
        return cycleTime != null ? cycleTime.getJobCount() : 0;

    }

    private static CycleTime findCycleTimeByNodeId(final String nodeId, final DynamoDBMapper dynamoDBMapper) {
        // lookup by nodeId
        final PaginatedQueryList<CycleTime> query = dynamoDBMapper.query(CycleTime.class,
                new DynamoDBQueryExpression<CycleTime>()
                        .withHashKeyValues(new CycleTime(nodeId, null, null)));
        if (query == null || query.isEmpty()) {
            return null;
        }
        // only 1 should exists
        return query.get(0);
    }
}

