package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.nio.charset.Charset;
import java.util.List;

import static com.dematic.labs.analytics.ingestion.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.ingestion.sparks.DriverUtils.getStreamingContext;

public class EventStreamStatistics {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamStatistics.class);
    // store state
    private static final String TMP_DIR = "/tmp/streaming_event_statistics";

    // functions
    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
    private static Function2<List<Long>, Optional<Long>, Optional<Long>>
            COMPUTE_RUNNING_SUM = (nums, current) -> {
        long sum = current.or(0L);
        for (long i : nums) {
            sum += i;
        }
        return Optional.of(sum);
    };

    public static void main(final String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint and Kinesis StreamName");
        }
        // url and stream name to pull events
        final String kinesisEndpoint = args[0];
        final String streamName = args[1];

        final Duration pollTime = Durations.seconds(1);
        // make Duration configurable
        final JavaStreamingContext streamingContext = getStreamingContext(kinesisEndpoint, streamName, pollTime);

        // calculate stream statistics
        final EventStreamStatistics stats = new EventStreamStatistics();

        final JavaDStream<byte[]> javaDStream = getJavaDStream(kinesisEndpoint, streamName, pollTime, streamingContext);

        stats.calculate(javaDStream, streamingContext);

        // Start the streaming context and await termination
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void calculate(final JavaDStream<byte[]> inputStream, final JavaStreamingContext streamingContext) {
        // Checkpointing must be enabled to use the updateStateByKey function
        streamingContext.checkpoint(TMP_DIR);
        LOGGER.info("checkpoint dir >{}<", TMP_DIR);
        // transform the byte[] (byte arrays are json) to a string to events
        final JavaDStream<Event> events =
                inputStream.map(
                        event -> EventUtils.jsonToEvent(new String(event, Charset.defaultCharset()))
                );
        // compute the count of events by node
        eventsByNodeSummation(events);
        // compute the count of events by order
        eventsByOrderSummation(events);
    }

    public void eventsByNodeSummation(final JavaDStream<Event> events) {
        final JavaPairDStream<Integer, Long> eventsByNode = events
                .mapToPair(event -> Tuple2.apply(event.getNodeId(), 1L))
                .reduceByKey(SUM_REDUCER)
                .updateStateByKey(COMPUTE_RUNNING_SUM);

        eventsByNode.foreachRDD(rdd -> {
            LOGGER.info("node counts: {}", rdd.take(100));
            System.out.println("node counts: " + rdd.take(100));
            return null;
        });
    }

    public void eventsByOrderSummation(final JavaDStream<Event> events) {
        final JavaPairDStream<Integer, Long> eventsByOrder = events
                .mapToPair(event -> Tuple2.apply(event.getOrderId(), 1L))
                .reduceByKey(SUM_REDUCER)
                .updateStateByKey(COMPUTE_RUNNING_SUM);

        eventsByOrder.foreachRDD(rdd -> {
            LOGGER.info("order counts: {}", rdd.take(100));
            System.out.println("order counts: " + rdd.take(100));
            return null;
        });
    }
}


