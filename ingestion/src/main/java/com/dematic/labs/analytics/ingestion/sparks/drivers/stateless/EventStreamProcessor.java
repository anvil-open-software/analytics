package com.dematic.labs.analytics.ingestion.sparks.drivers.stateless;

import com.dematic.labs.analytics.common.sparks.DriverConfig;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;

/**
 * Generic interface so the same launch utility can be used.
 *
 * Old school, probably could redo this functionally...
 */

public interface EventStreamProcessor<T> extends Serializable {
    void processEvents(final DriverConfig driverConfig, final JavaDStream<T> javaDStream);
}
