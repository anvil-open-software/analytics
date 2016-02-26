package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.toolkit.communication.Event;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public final class AggregateFunctions implements Serializable {
    private AggregateFunctions() {
    }

    // aggregate event to bucket
    public static final class AggregateEventToBucketFunction implements PairFunction<Event, String, Long> {
        private final TimeUnit timeUnit;

        public AggregateEventToBucketFunction(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        @Override
        public Tuple2<String, Long> call(final Event event) throws Exception {
            // Downsampling: where we reduce the event’s ISO 8601 timestamp down to timeUnit precision,
            // so for instance “2015-06-05T12:54:43.064528” becomes “2015-06-05T12:54:00.000000” for minute.
            // This downsampling gives us a fast way of bucketing or aggregating events via this downsampled key
            return new Tuple2<>(event.aggregateBy(timeUnit), 1L);
        }
    }
}
