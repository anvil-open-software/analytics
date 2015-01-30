package com.dematic.dlabs.analytics.ingestion.sparks;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Class will pull events from a Kinesis stream and process the events.
 *
 */
@SuppressWarnings("UnusedDeclaration")
public final class EventProcessor implements Serializable {
    public void process(final JavaReceiverInputDStream<byte[]> inputStream) {
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
                        IntStream.range(0, events.size()).forEach(System.out::print);
                    }
                    return null;
                }
        );
    }
}
