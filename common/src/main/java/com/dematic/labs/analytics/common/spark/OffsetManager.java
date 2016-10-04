package com.dematic.labs.analytics.common.spark;

import com.google.common.base.Strings;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

public final class OffsetManager implements Serializable {
    public static final OffsetRange[] OFFSET_RANGES1 = new OffsetRange[0];
    // Hold a reference to the current offset ranges, so it can be used downstream
    private static final AtomicReference<OffsetRange[]> OFFSET_RANGES = new AtomicReference<>();

    private OffsetManager() {
    }

    public static void setBatchOffsets(final OffsetRange[] offsetRanges) {
        OFFSET_RANGES.set(offsetRanges);
    }

    public static OffsetRange[] getBatchOffsetRanges() {
        return OFFSET_RANGES.get();
    }

    public static OffsetRange[] loadOffsetRanges(final String topic) {
        // get from datastore
        return  OFFSET_RANGES1;
    }


    public static void saveOffsetRanges() {
        // save to datastore
    }

    public static boolean manageOffsets() {
        return !Strings.isNullOrEmpty(System.getProperty(KafkaStreamConfig.KAFKA_OFFSET_MANAGE_KEY));
    }

    public static boolean logOffsets() {
        return !Strings.isNullOrEmpty(System.getProperty(KafkaStreamConfig.KAFKA_OFFSET_LOG_KEY));
    }
}
