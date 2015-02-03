package com.dematic.labs.analytics.ingestion.sparks;

import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Class will pull events from a Kinesis stream and process the events.
 */
@SuppressWarnings("UnusedDeclaration")
public final class EventProcessor implements Serializable {
    private transient int eventsProcessed = 0;

    public void process(final JavaDStream<byte[]> inputStream) {
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
                        events.stream().forEach(System.out::print);
                        eventsProcessed = events.size();
                    }
                    return null;
                }
        );
    }

    // temporary method used for testing, this will be removed
    public int getEventsProcessed() {
        return eventsProcessed;
    }
}
