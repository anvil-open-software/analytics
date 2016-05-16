package com.dematic.labs.analytics.ingestion.spark.drivers.signal.kinesis;

import com.dematic.labs.analytics.common.spark.DriverConsts;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.aws.KinesisStreamRule;
import com.jayway.awaitility.Awaitility;
import org.apache.spark.streaming.StreamingContext;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Ignore
public final class ComputeCumulativeMetricsTest {
    // create a tmp dir
    private TemporaryFolder folder = new TemporaryFolder();
    // setup Kinesis
    private final KinesisStreamRule kinesisStreamRule = new KinesisStreamRule();
    // load system properties from file and Rules
    @Rule
    public final TestRule systemPropertyRule =
            RuleChain.outerRule(new SystemPropertyRule()).around(kinesisStreamRule).around(folder);

    @Test
    public void computeMetrics() throws InterruptedException {
        System.setProperty("spark.cassandra.auth.username", "spark");
        System.setProperty("spark.cassandra.auth.password", "spark123");
        System.setProperty("spark.cassandra.connection.host", "10.40.217.180");

        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        final String cassandraHost = "10.40.217.180";
        // append user name to ensure tables are unique to person running tests to avoid collisions
        final String keyspace = "mm_signal";//System.getProperty("user.name") + "_";
        // set the checkpoint dir
        final String checkpointDir = folder.getRoot().getAbsolutePath();
        System.setProperty(DriverConsts.SPARK_CHECKPOINT_DIR, checkpointDir);
        System.setProperty("spark.driver.allowMultipleContexts", "true");
        // start sparks in a separate thread
        try {
            final ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.submit(() -> ComputeCumulativeMetrics.main(new String[]{kinesisEndpoint, kinesisInputStream,
                    cassandraHost, keyspace, "local[*]", "10"}));

            // give spark some time to start
            TimeUnit.SECONDS.sleep(20);
            // ensure all use-cases succeed
            checkSavedEvents("", "");
        } finally {
            try {
                StreamingContext.getActive().get().stop(true, false);
            } catch (final Throwable ignore) {
            }
            // delete cassandra tables
        }
    }

    private void checkSavedEvents(final String dynamoDBEndpoint, final String tablePrefix) {

        // set the defaults timeouts
        Awaitility.setDefaultTimeout(30, TimeUnit.MINUTES);

        // poll dynamoDB CT table and ensure job count == 1
        Awaitility.with().pollInterval(10, TimeUnit.SECONDS).and().with().
                pollDelay(10, TimeUnit.SECONDS).await().
                until(() -> Assert.assertEquals(1, 3));
    }
}
