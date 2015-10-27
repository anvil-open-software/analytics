package com.dematic.labs.analytics.ingestion.drivers;

import com.dematic.labs.analytics.common.sparks.DematicSparkSession;
import com.dematic.labs.analytics.common.sparks.DriverConsts;
import com.dematic.labs.analytics.common.sparks.DriverUtils;
import org.apache.spark.streaming.Duration;
import org.junit.Test;

import static com.dematic.labs.analytics.ingestion.sparks.drivers.EventStreamAggregator.EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public final class DematicSessionTest {

    @Test
    public void testSession() {
        String[] args = {"https://kinesis.$DYNAMODB_AWS_REGION.amazonaws.com",
                "test_stream",
                "https://dynamodb.$DYNAMODB_AWS_REGION.amazonaws.com",
                "test_",
                "5",
                "MINUTES"};

        String checkpointDir = "/tmp/spark/test";
        DematicSparkSession session = new DematicSparkSession(EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME, args);
        System.clearProperty(DriverConsts.SPARK_CHECKPOINT_DIR);
        session.setCheckPointDirectoryFromSystemProperties(false);
        assertNull(session.getCheckPointDir());
        System.setProperty(DriverConsts.SPARK_CHECKPOINT_DIR, checkpointDir);
        session.setCheckPointDirectoryFromSystemProperties(true);
        assertEquals(checkpointDir, session.getCheckPointDir());
    }

    @Test
    public void testKinesisWindowProperties() {
        System.clearProperty(DriverConsts.SPARK_KINESIS_CHECKPOINT_WINDOW_IN_SECONDS);
        Duration defaultDuration = DriverUtils.getKinesisCheckpointWindow();
        assertEquals(defaultDuration.milliseconds(), 30 * 1000l);

        System.setProperty(DriverConsts.SPARK_KINESIS_CHECKPOINT_WINDOW_IN_SECONDS, "47");
        Duration newDuration = DriverUtils.getKinesisCheckpointWindow();
        assertEquals(newDuration.milliseconds(), 47 * 1000l);
    }

}