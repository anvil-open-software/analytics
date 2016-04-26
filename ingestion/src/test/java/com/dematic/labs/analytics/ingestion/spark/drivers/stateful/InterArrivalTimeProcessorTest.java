package com.dematic.labs.analytics.ingestion.spark.drivers.stateful;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.dematic.labs.analytics.common.spark.DriverConsts;
import com.dematic.labs.analytics.ingestion.spark.tables.Bucket;
import com.dematic.labs.analytics.ingestion.spark.tables.BucketUtils;
import com.dematic.labs.analytics.ingestion.spark.tables.InterArrivalTime;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.aws.KinesisStreamRule;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.jayway.awaitility.Awaitility;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.ingestion.spark.drivers.stateful.InterArrivalTimeProcessor.INTER_ARRIVAL_TIME_LEASE_TABLE_NAME;
import static com.dematic.labs.toolkit.aws.Connections.deleteDynamoTable;
import static com.dematic.labs.toolkit.aws.Connections.getAmazonDynamoDBClient;
import static org.junit.Assert.assertEquals;

@Ignore
public final class InterArrivalTimeProcessorTest {
    // used to create the IAT buckets
    private static final String AVG_INTER_ARRIVAL_TIME = "10";
    private static final String NODE_ID = "node_10";
    // create a tmp dir
    private TemporaryFolder folder = new TemporaryFolder();
    // setup Kinesis
    private final KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();
    // load system properties from file and Rules
    @Rule
    public final TestRule systemPropertyRule =
            RuleChain.outerRule(new SystemPropertyRule()).around(kinesisStreamRule).around(folder);

    @Test
    public void calculateInterArrivalTimes() throws InterruptedException {
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

            // ensure all use-cases succeed
            checkSingleEventIAT(dynamoDBEndpoint, userNamePrefix, NODE_ID);
            checkMultipleEventsIAT(dynamoDBEndpoint, userNamePrefix, NODE_ID);
            checkIATBetweenBatches(NODE_ID);

        } finally {
            // delete dynamo tables
            final AmazonDynamoDBClient amazonDynamoDBClient = getAmazonDynamoDBClient(dynamoDBEndpoint);
            try {
                final String tableName = String.format("%s%s", userNamePrefix, InterArrivalTime.TABLE_NAME);
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

    private void checkSingleEventIAT(final String dynamoDBEndpoint, final String tablePrefix, final String nodeId) {
        // push one event to kinesis
        kinesisStreamRule.pushEventsToKinesis(EventUtils.generateEvents(1, nodeId));

        // set the defaults timeouts
        Awaitility.setDefaultTimeout(3, TimeUnit.MINUTES);
        // poll dynamoDB IAT table and ensure error count == 0, error count will be 0 because we only get 1 event in
        // the batch and no previous events occurred, may need to re-visit this logic in the future
        Awaitility.with().pollInterval(10, TimeUnit.SECONDS).and().with().
                pollDelay(10, TimeUnit.SECONDS).await().
                until(() -> assertEquals(0, getIATErrorCount(dynamoDBEndpoint, tablePrefix, nodeId)));
    }

    private void checkMultipleEventsIAT(final String dynamoDBEndpoint, final String tablePrefix, final String nodeId) {
        // setup buckets
        final int numberOfEventsToPush = 100;
        final int bucketLowerBoundry = Integer.parseInt(AVG_INTER_ARRIVAL_TIME);
        final int bucketHigherBoundry = bucketLowerBoundry + 2;

        // push one event to kinesis
        kinesisStreamRule.pushEventsToKinesis(EventUtils.generateEvents(numberOfEventsToPush, nodeId,
                bucketLowerBoundry));

        // set the defaults timeouts
        Awaitility.setDefaultTimeout(3, TimeUnit.MINUTES);
        // poll dynamoDB IAT table and ensure error count == 1, error count will be 1 because we only get 1 event in
        // the batch and no previous events occurred, may need to re-visit this logic in the future
        Awaitility.with().pollInterval(10, TimeUnit.SECONDS).and().with().
                pollDelay(10, TimeUnit.SECONDS).await().
                until(() -> assertEquals(99, getIATCountByBucket(dynamoDBEndpoint, tablePrefix, nodeId,
                        bucketLowerBoundry, bucketHigherBoundry)));
    }

    private void checkIATBetweenBatches(final String nodeId) {

    }

    private long getIATErrorCount(final String dynamoDBEndpoint, final String tablePrefix, final String nodeId) {
        final AmazonDynamoDBClient dynamoDBClient = Connections.getAmazonDynamoDBClient(dynamoDBEndpoint);
        final DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(dynamoDBClient,
                new DynamoDBMapperConfig(withTableNamePrefix(tablePrefix)));
        final InterArrivalTime interArrivalTime = findInterArrivalTime(nodeId, dynamoDBMapper);
        return interArrivalTime == null ? -1 : interArrivalTime.getErrorCount();
    }

    private long getIATCountByBucket(final String dynamoDBEndpoint, final String tablePrefix, final String nodeId,
                                     final int lowerBoundry, final int upperBoundry) {
        final AmazonDynamoDBClient dynamoDBClient = Connections.getAmazonDynamoDBClient(dynamoDBEndpoint);
        final DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(dynamoDBClient,
                new DynamoDBMapperConfig(withTableNamePrefix(tablePrefix)));
        final InterArrivalTime interArrivalTime = findInterArrivalTime(nodeId, dynamoDBMapper);
        if (interArrivalTime == null) {
            return 0;
        }
        final Set<String> buckets = interArrivalTime.getBuckets();
        for (final String bucket : buckets) {
            final Bucket interArrivalTimeBucket = BucketUtils.jsonToBucketUnchecked(bucket);
            final int bucketLowerBoundry = interArrivalTimeBucket.getLowerBoundry();
            final int bucketUpperBoundry = interArrivalTimeBucket.getUpperBoundry();
            if (lowerBoundry == bucketLowerBoundry && upperBoundry == bucketUpperBoundry) {
                return interArrivalTimeBucket.getCount();
            }
        }
        return 0;
    }

    private static InterArrivalTime findInterArrivalTime(final String nodeId, final DynamoDBMapper dynamoDBMapper) {
        // lookup buckets by nodeId
        final PaginatedQueryList<InterArrivalTime> query = dynamoDBMapper.query(InterArrivalTime.class,
                new DynamoDBQueryExpression<InterArrivalTime>()
                        .withHashKeyValues(new InterArrivalTime(nodeId)));
        if (query == null || query.isEmpty()) {
            return null;
        }
        // only 1 should exists
        return query.get(0);
    }
}
