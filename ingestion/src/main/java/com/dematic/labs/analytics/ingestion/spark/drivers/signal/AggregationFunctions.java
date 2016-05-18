package com.dematic.labs.analytics.ingestion.spark.drivers.signal;

import com.dematic.labs.analytics.common.spark.CassandraDriverConfig;
import com.dematic.labs.toolkit.communication.Signal;
import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import java.io.Serializable;
import java.util.List;

public final class AggregationFunctions implements Serializable {
    private AggregationFunctions() {
    }

    public static final class ComputeMovingSignalAggregation implements Function4<Time, String, Optional<List<Signal>>,
            State<SignalAggregation>, Optional<SignalAggregation>> {
        @SuppressWarnings("unused") // todo: will be used when retrieving data from cassandra
        private final CassandraDriverConfig driverConfig;

        public ComputeMovingSignalAggregation(final CassandraDriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public Optional<SignalAggregation> call(final Time time, final String opcTagId,
                                                final Optional<List<Signal>> signals,
                                                final State<SignalAggregation> state) throws Exception {
            final SignalAggregation signalAggregation;
            // check for saved state
            if (state.exists()) {
                // get existing signal aggregations
                signalAggregation = state.get();
                // check if timing out
                final boolean timingOut = state.isTimingOut();
                if (timingOut) {
                    // no state has been updated for timeout amount of time, just calculate metrics and return
                    if (signals.isPresent()) {
                        signalAggregation.computeAggregations(signals.get());
                        return Optional.of(signalAggregation);
                    } else {
                        return Optional.absent();
                    }
                }
                // calculate and update
                signalAggregation.computeAggregations(signals.get());
                state.update(signalAggregation);
            } else {
                // create initial state,
                // todo: load from cassandra
                signalAggregation = new SignalAggregation(opcTagId);
                signalAggregation.computeAggregations(signals.get());
                state.update(signalAggregation);
            }
            return Optional.of(signalAggregation);
        }
    }
}