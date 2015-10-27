package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.analytics.common.sparks.DriverConfig;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;

/**
 * Generic interface so the same launch utility can be used.
 *
 * Old school, probably could redo this functionally...
 */

public interface EventStreamProcessor<T> extends Serializable {

     void processEvents(DriverConfig driverConfig, JavaDStream<T> javaDStream);
}
