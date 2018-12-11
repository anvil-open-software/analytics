/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.ingestion.spark.drivers.signal;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static com.dematic.labs.analytics.common.communication.SignalUtils.toTimestampFromInstance;
import static com.dematic.labs.analytics.common.communication.SignalUtils.toInstantFromTimestamp;

@SuppressWarnings("unused")
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
