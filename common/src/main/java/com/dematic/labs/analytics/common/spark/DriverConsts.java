/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.common.spark;

/**
 * Keep track of all parameters here
 */
public interface DriverConsts {
    String SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"; // number of shuffle partition per query
    String SPARK_OUTPUT_MODE = "spark.output.mode";
    String SPARK_WATERMARK_TIME = "spark.watermark.time"; // keeps track of how long to keep data
    String SPARK_QUERY_TRIGGER = "spark.query.trigger";
    String SPARK_CHECKPOINT_DIR = "spark.checkpoint.dir";
    String SPARK_STREAMING_CHECKPOINT_DIR = "spark.sql.streaming.checkpointLocation";
    String SPARK_PARQUET_DIR = "spark.parquet.dir";
    String SPARK_DRIVER_VALIDATE_COUNTS = "dematiclabs.driver.validate.counts";
    String SPARK_QUERY_STATISTICS = "dematiclabs.driver.query.statistics";

    String SPARK_CLUSTER_ID="dematiclabs.spark.cluster_id";
}
