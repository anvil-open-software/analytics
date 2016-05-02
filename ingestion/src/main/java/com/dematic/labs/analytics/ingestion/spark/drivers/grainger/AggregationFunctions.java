package com.dematic.labs.analytics.ingestion.spark.drivers.grainger;

import com.dematic.labs.analytics.common.spark.DriverConfig;
import com.dematic.labs.toolkit.communication.Signal;
import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import java.io.Serializable;
import java.util.List;

final class AggregationFunctions implements Serializable {
    private AggregationFunctions() {
    }

    static final class ComputeMovingSignalAggregation implements Function4<Time, String, Optional<List<Signal>>,
            State<SignalAggregation>, Optional<SignalAggregation>> {
        private final DriverConfig driverConfig;

        ComputeMovingSignalAggregation(final DriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public Optional<SignalAggregation> call(final Time time, final String opcTagId,
                                                final Optional<List<Signal>> signals,
                                                final State<SignalAggregation> state) throws Exception {
            final SignalAggregation signalAggregation;
            // check for saved state
            if (state.exists()) {
                signalAggregation = state.get();
                signalAggregation.computeAggregations(signals.get());

            } else {
                // create initial state
                signalAggregation = new SignalAggregation();
                signalAggregation.computeAggregations(signals.get());
                state.update(signalAggregation);
            }
            return Optional.of(signalAggregation);
        }
    }
}
