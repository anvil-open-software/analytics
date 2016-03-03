package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.common.spark.DriverConfig;
import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Optional;
import com.google.common.collect.Multimap;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;

import java.util.UUID;

public final class CycleTimeFunctions {
    private CycleTimeFunctions() {
    }

    public static final class createModel implements Function3<String, Optional<Multimap<UUID, Event>>,
            State<CycleTimeState>, Optional<CycleTime>> {

        private final DriverConfig driverConfig;

        public createModel(final DriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public Optional<CycleTime> call(final String nodeId, final Optional<Multimap<UUID, Event>> map,
                                        final State<CycleTimeState> state) throws Exception {
            return null;
        }
    }
}

