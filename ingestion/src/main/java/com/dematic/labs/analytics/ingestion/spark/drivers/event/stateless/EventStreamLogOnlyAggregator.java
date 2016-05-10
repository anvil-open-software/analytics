package com.dematic.labs.analytics.ingestion.spark.drivers.event.stateless;

import com.dematic.labs.analytics.common.spark.DriverConfig;
import com.dematic.labs.analytics.ingestion.spark.drivers.event.AggregateFunctions.AggregateEventToBucketFunction;
import com.dematic.labs.analytics.common.spark.StreamFunctions.CreateStreamingContextFunction;
import com.dematic.labs.analytics.ingestion.spark.tables.event.EventAggregator;
import com.dematic.labs.toolkit.communication.Event;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;

import static com.dematic.labs.analytics.common.spark.CalculateFunctions.*;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;

/**
 * Aggregator with dynamodb bucket writes removed.
 */
public final class EventStreamLogOnlyAggregator implements Serializable {
    private static final String EVENT_STREAM_LOGGING_LEASE_TABLE_NAME = EventAggregator.TABLE_NAME + "_Log_LT";
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamLogOnlyAggregator.class);

    // event stream processing function
    private static final class AggregateEventFunction implements VoidFunction<JavaDStream<byte[]>> {
        private final DriverConfig driverConfig;

        AggregateEventFunction(final DriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(JavaDStream<byte[]> javaDStream) throws Exception {
            // transform the byte[] (byte arrays are json) to a string to events, and ensure distinct within stream and
            // filter against cache to ensure no duplicates
            final JavaDStream<Event> eventStream =
                    javaDStream.map(event -> jsonToEvent(new String(event, Charset.defaultCharset())))
                            .transform((Function<JavaRDD<Event>, JavaRDD<Event>>) JavaRDD::distinct);

            // map to pairs and aggregate by key
            final JavaPairDStream<String, Long> aggregates = eventStream
                    .mapToPair(new AggregateEventToBucketFunction(driverConfig.getTimeUnit()))
                    .reduceByKey(SUM_REDUCER);

            // save counts
            aggregates.foreachRDD(rdd -> {
                final List<Tuple2<String, Long>> collect = rdd.collect();
                for (final Tuple2<String, Long> bucket : collect) {
                    LOGGER.info(bucket.toString());
                }
                return null;
            });
        }
    }

    public static void main(final String[] args) {
        // set the configuration and checkpoint dir
        final DriverConfig config = new DriverConfig(EVENT_STREAM_LOGGING_LEASE_TABLE_NAME, args);
        config.setCheckPointDirectoryFromSystemProperties(true);
        // create the table, if it does not exist
          // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext =
                JavaStreamingContext.getOrCreate(config.getCheckPointDir(),
                        new CreateStreamingContextFunction(config, new AggregateEventFunction(config)));

        // Start the streaming context and await termination
        LOGGER.info("starting Event Logging Only Aggregator Driver with master URL >{}<",
                streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }
}
