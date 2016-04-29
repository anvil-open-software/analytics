package com.dematic.labs.analytics.ingestion.spark.drivers.grainger;

import com.dematic.labs.toolkit.communication.Signal;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

final class AggregationFunctions implements Serializable {
    private AggregationFunctions() {
    }

    static class ComputeSignalAggregation extends Aggregator<Signal, Long, SignalAggregation> {
        private final SignalAggregation signalAggregation;

        public ComputeSignalAggregation() {
            signalAggregation = new SignalAggregation();
        }

        @Override
        public Long zero() {
            return 0L;
        }

        @Override
        public Long reduce(final Long value, final Signal signal) {
            return value + signal.getValue();
        }

        @Override
        public SignalAggregation finish(final Long reduction) {
            return signalAggregation.computeAggregations(reduction);
        }

        @Override
        public Long merge(final Long valueOne, final Long valueTwo) {
            return valueOne + valueTwo;
        }
    }
}
