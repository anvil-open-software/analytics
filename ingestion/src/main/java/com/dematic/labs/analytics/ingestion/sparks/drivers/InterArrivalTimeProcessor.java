package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.analytics.common.sparks.DriverConfig;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTimeBucket;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static com.dematic.labs.analytics.ingestion.sparks.drivers.Functions.*;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;

public final class InterArrivalTimeProcessor implements Serializable {
    public static final String INTER_ARRIVAL_TIME_LEASE_TABLE_NAME = InterArrivalTimeBucket.TABLE_NAME + "_LT";
    private static final Logger LOGGER = LoggerFactory.getLogger(InterArrivalTimeProcessor.class);

    // functions
    public static void main(final String[] args) {
        // set the configuration and checkpoint dir
        final DriverConfig config = new DriverConfig(INTER_ARRIVAL_TIME_LEASE_TABLE_NAME, args);
        config.setCheckPointDirectoryFromSystemProperties(true);
        // create the table, if it does not exist
        createDynamoTable(config.getDynamoDBEndpoint(), InterArrivalTimeBucket.class, config.getDynamoPrefix());
        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext =
                JavaStreamingContext.getOrCreate(config.getCheckPointDir(), new CreateStreamingContextFunction(config));

        // Start the streaming context and await termination
        LOGGER.info("starting Inter-ArrivalTime Driver with master URL >{}<", streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }
}
