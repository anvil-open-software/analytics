package com.dematic.labs.analytics.ingestion.sparks.drivers;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;

import static com.dematic.labs.analytics.ingestion.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.ingestion.sparks.DriverUtils.getStreamingContext;

/**
 * Counts the number of events.
 */
public final class EventCount implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventCount.class);

    private EventCount() {
    }

    public static void main(final String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException(String.format("Driver requires Kinesis Endpoint and Kinesis StreamName"));
        }
        // url and stream name to pull events
        final String endpointUrl = args[0];
        final String streamName = args[1];

        final Duration pollTime = Durations.seconds(2);
        // make Duration configurable
        final JavaStreamingContext streamingContext = getStreamingContext(endpointUrl, streamName, pollTime);

        // count events
        final EventCount eventCount = new EventCount();
        eventCount.countEvents(getJavaDStream(endpointUrl, streamName, pollTime, streamingContext));
        // Start the streaming context and await termination
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void countEvents(final JavaDStream<byte[]> inputStream) {
        // transform the byte[] (byte arrays are json) to a string
        final JavaDStream<String> eventMap =
                inputStream.map(
                        event -> new String(event, Charset.defaultCharset())
                );
        // for each distributed data set, send to an output stream, for now we just log
        eventMap.foreachRDD(
                // rdd = distributed data set
                rdd -> {
                    if (rdd.count() > 0) {
                        final List<String> events = rdd.collect();
                        events.stream().forEach(LOGGER::info);
                        LOGGER.info("received >{}< events", events.size());
                    }
                    return null;
                }
        );
    }
}