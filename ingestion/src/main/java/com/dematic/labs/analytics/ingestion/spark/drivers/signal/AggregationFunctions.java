/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.ingestion.spark.drivers.signal;

import com.dematic.labs.analytics.common.communication.Signal;
import com.dematic.labs.analytics.common.spark.CassandraDriverConfig;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public final class AggregationFunctions implements Serializable {
    private AggregationFunctions() {
    }

    public static final class ComputeMovingSignalAggregationByOpcTagIdAndAggregation
            implements Function4<Time, Tuple2<Long, Date>, Optional<List<Signal>>, State<SignalAggregation>, Optional<SignalAggregation>> {
        @SuppressWarnings("unused") // todo: configure when needed
        private final CassandraDriverConfig driverConfig;

        public ComputeMovingSignalAggregationByOpcTagIdAndAggregation(final CassandraDriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @SuppressWarnings("Duplicates")
        @Override
        public Optional<SignalAggregation> call(final Time time, final Tuple2<Long, Date> key,
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
                signalAggregation = new SignalAggregation(key._1(), key._2());
                signalAggregation.computeAggregations(signals.get());
                state.update(signalAggregation);
            }
            return Optional.of(signalAggregation);
        }
    }
}
