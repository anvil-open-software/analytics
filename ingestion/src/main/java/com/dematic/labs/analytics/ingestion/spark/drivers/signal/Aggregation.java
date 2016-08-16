package com.dematic.labs.analytics.ingestion.spark.drivers.signal;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static com.dematic.labs.toolkit.communication.SignalUtils.toInstantFromJavaUtilDate;
import static com.dematic.labs.toolkit.communication.SignalUtils.toJavaUtilDateFromInstance;

public enum Aggregation {
    HOUR(ChronoUnit.HOURS),
    MINUTE(ChronoUnit.MINUTES);

    private final ChronoUnit chronoUnit;

    Aggregation(final ChronoUnit chronoUnit) {
        this.chronoUnit = chronoUnit;
    }

    public Date time(final Date time) {
        // todo: investigate conversions more
        return toJavaUtilDateFromInstance(truncate(time, chronoUnit));
    }

    private static Instant truncate(final Date time, final ChronoUnit chronoUnit) {
        return toInstantFromJavaUtilDate(time).truncatedTo(chronoUnit);
    }
}
