package com.dematic.labs.analytics.common.spark;

/**
 * Keep track of all parameters here
 */
public interface DriverConsts {
    String SPARK_QUERY_TRIGGER = "spark.query.trigger";
    String SPARK_CHECKPOINT_DIR = "spark.checkpoint.dir";
    String SPARK_STREAMING_CHECKPOINT_DIR = "spark.sql.streaming.checkpointLocation";
    String SPARK_DRIVER_VALIDATE_COUNTS = "dematiclabs.driver.validate.counts";
    String SPARK_QUERY_STATISTICS = "dematiclabs.driver.query.statistics";
}
