package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.common.spark.DriverConfig;
import com.dematic.labs.analytics.ingestion.sparks.tables.BucketUtils;
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
        public Optional<CycleTime> call(final String nodeId, final Optional<Multimap<UUID, Event>> jobs,
                                        final State<CycleTimeState> state) throws Exception {
            if (!jobs.isPresent()) {
                Optional.absent();
            }

            final CycleTimeState cycleTimeState;
            if (state.exists()) {
                cycleTimeState = state.get();

                final boolean timingOut = state.isTimingOut();
                if (timingOut) {
                    // no state has been updated for timeout amount of time, that is, no events associated to the node
                    // has been updated within the configured timeout, calculate any error cases, uuid's without pairs
                    return Optional.of(cycleTimeState.createModel());
                } else {
                    // add new UUID grouping to the map
                    cycleTimeState.updateEvents(jobs.get());
                    state.update(cycleTimeState);
                }
            } else {
                // create and add the initial state, todo: check the db for saved state
                cycleTimeState = new CycleTimeState(nodeId, jobs.get(), BucketUtils.createCycleTimeBuckets(4, 10));
                state.update(cycleTimeState);
            }
            return Optional.of(cycleTimeState.createModel());
        }
    }
}

