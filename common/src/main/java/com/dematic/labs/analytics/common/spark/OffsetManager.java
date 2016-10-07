package com.dematic.labs.analytics.common.spark;

import com.google.common.base.Strings;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

public final class OffsetManager implements Serializable {
    public static final String TABLE_NAME = "offsets";

    public static String createTableCql(final String keyspace) {
        return String.format("CREATE TABLE if not exists %s.%s (" +
                " topic text," +
                " partition bigint," +
                " fromOffset bigint," +
                " toOffset bigint," +
                " PRIMARY KEY (topic), partition))" +
                " WITH CLUSTERING ORDER BY (partition DESC);", keyspace, TABLE_NAME);
    }

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
        return null;

    }

    public static void saveOffsetRanges(final OffsetRange[] offsetRanges) {
        // save to datastore
    }

    public static boolean manageOffsets() {
        return !Strings.isNullOrEmpty(System.getProperty(KafkaStreamConfig.KAFKA_OFFSET_MANAGE_KEY));
    }

    public static boolean logOffsets() {
        return !Strings.isNullOrEmpty(System.getProperty(KafkaStreamConfig.KAFKA_OFFSET_LOG_KEY));
    }
}
