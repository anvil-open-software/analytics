package com.dematic.labs.analytics.ingestion.spark.drivers.event.stateless;

import com.dematic.labs.analytics.ingestion.spark.tables.event.EventAggregator;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;

/**
 * Wrapper for EventStreamAggregator with checkpoint turned on. Does not have any stateful processing.
 * <p>
 * Probably could have just turned on checkpointing in EventStreamAggregator
 * Also the argument parsing probably could be refactored to be shared among all EventStreamAggregators
 */
public final class EventStreamCheckpointedAggregator implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamCheckpointedAggregator.class);

    private static final String EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME = EventAggregator.TABLE_NAME + "_Checkpoint_LT";

    public static void main(final String[] args) {
        try {
            final AggregationDriverConfig session = new AggregationDriverConfig(EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME, args);
            session.setCheckPointDirectoryFromSystemProperties(true);

            // create the table, if it does not exist
            createDynamoTable(session.getDynamoDBEndpoint(), EventAggregator.class, session.getDynamoPrefix());

            final SimpleEventStreamAggregator eventStreamAggregator = new SimpleEventStreamAggregator();
            final JavaStreamingContext streamingContext =
                    AggregationDriverUtils.initializeCheckpointedSparkSession(session, null, eventStreamAggregator);
            streamingContext.start();
            LOGGER.info("spark state: {}", streamingContext.getState().name());
            streamingContext.awaitTermination();
        } catch (final Throwable any) {
            any.printStackTrace();
            throw any;
        }
    }
}