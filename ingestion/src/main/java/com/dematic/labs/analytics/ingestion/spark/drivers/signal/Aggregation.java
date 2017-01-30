package com.dematic.labs.analytics.ingestion.spark.drivers.signal;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static com.dematic.labs.toolkit.helpers.bigdata.communication.SignalUtils.toInstantFromTimestamp;
import static com.dematic.labs.toolkit.helpers.bigdata.communication.SignalUtils.toTimestampFromInstance;

public enum Aggregation {
    HOUR(ChronoUnit.HOURS),
    MINUTE(ChronoUnit.MINUTES);

    private final ChronoUnit chronoUnit;

    Aggregation(final ChronoUnit chronoUnit) {
        this.chronoUnit = chronoUnit;
    }

    public Timestamp time(final Timestamp time) {
        return toTimestampFromInstance(truncate(time, chronoUnit));
    }

    private static Instant truncate(final Timestamp time, final ChronoUnit chronoUnit) {
        return toInstantFromTimestamp(time).truncatedTo(chronoUnit);
    }
}
