package com.dematic.labs.analytics.ingestion.spark.drivers.event.stateless;

import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;

/**
 * Generic interface so the same launch utility can be used.
 *
 * Old school, probably could redo this functionally...
 */

interface EventStreamProcessor<T> extends Serializable {
    void processEvents(final AggregationDriverConfig driverConfig, final JavaDStream<T> javaDStream);
}
