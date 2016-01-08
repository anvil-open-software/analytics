package com.dematic.labs.analytics.common.sparks;

/**
 * Keep track of all parameters here
 */
public interface DriverConsts {
    String SPARK_CHECKPOINT_DIR = "spark.checkpoint.dir";
    String SPARK_KINESIS_CHECKPOINT_WINDOW_IN_SECONDS =  "spark.kinesis.checkpoint.window";

    // for turning off explicit bucket writes, does not control lease table checkpointing.
    String SPARK_DRIVER_SKIP_DYNAMODB_WRITE = "dematiclabs.driver.dynamodb.skipwrite";
}
