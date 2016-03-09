package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Optional;
import com.google.common.collect.Multimap;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import java.io.Serializable;
import java.util.UUID;

import static com.dematic.labs.analytics.ingestion.sparks.tables.BucketUtils.createCycleTimeBuckets;

public final class CycleTimeFunctions implements Serializable {
    private CycleTimeFunctions() {
    }

    public static final class createModel implements Function4<Time, String, Optional<Multimap<UUID, Event>>,
            State<CycleTimeState>, Optional<CycleTime>> {

        private final CycleTimeDriverConfig driverConfig;

        public createModel(final CycleTimeDriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public Optional<CycleTime> call(final Time time, final String nodeId, final Optional<Multimap<UUID, Event>> jobs,
                                        final State<CycleTimeState> state) throws Exception {
            final CycleTimeState cycleTimeState;
            if (state.exists()) {
                cycleTimeState = state.get();

                final boolean timingOut = state.isTimingOut();
                if (timingOut) {
                    // no state has been updated for timeout amount of time, that is, no events associated to the node
                    // has been updated within the configured timeout, calculate any error cases, uuid's without pairs
                    return Optional.of(cycleTimeState.createModel(true));
                } else {
                    // add new UUID grouping to the map
                    cycleTimeState.updateJobs(jobs.get());
                    state.update(cycleTimeState);
                }
            } else {
                // create and add the initial state, todo: check the db for saved state
                cycleTimeState = new CycleTimeState(nodeId, jobs.get(),
                        createCycleTimeBuckets(asInt(driverConfig.getBucketIncrementer()),
                                asInt(driverConfig.getBucketSize())));
                state.update(cycleTimeState);
            }
            return Optional.of(cycleTimeState.createModel(false));
        }
    }

    private static int asInt(final String intString){
        return Integer.parseInt(intString);
    }
}

