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
    // store state,
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

    private static Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>
            SUM_AND_COUNT_REDUCER = (x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2());

    private static Function2<List<Tuple2<Double, Long>>, Optional<Tuple2<Double, Long>>, Optional<Tuple2<Double, Long>>>
            COMPUTE_RUNNING_AVG = (sums, current) -> {
        Tuple2<Double, Long> avgAndCount = current.or(new Tuple2<>(0.0, 0L));

        for (final Tuple2<Double, Long> sumAndCount : sums) {
            final double avg = avgAndCount._1();
            final long avgCount = avgAndCount._2();

            final double sum = sumAndCount._1();
            final long sumCount = sumAndCount._2();

            final Long countTotal = avgCount + sumCount;
            final Double newAvg = ((avgCount * avg) + (sumCount * sum / sumCount)) / countTotal;

            avgAndCount = new Tuple2<>(newAvg, countTotal);
        }
        return Optional.of(avgAndCount);
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
        final JavaStreamingContext streamingContext = getStreamingContext(kinesisEndpoint, streamName,
                pollTime);

        // Checkpointing must be enabled to use the updateStateByKey function
        streamingContext.checkpoint(TMP_DIR);
        LOGGER.info("checkpoint dir >{}<", TMP_DIR);

        // calculate stream statistics
        final EventStreamStatistics stats = new EventStreamStatistics();

        final JavaDStream<byte[]> javaDStream = getJavaDStream(kinesisEndpoint, streamName, pollTime, streamingContext);

        stats.calculate(javaDStream);

        // Start the streaming context and await termination
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void calculate(final JavaDStream<byte[]> inputStream) {
        // transform the byte[] (byte arrays are json) to a string to events
        final JavaDStream<Event> events =
                inputStream.map(
                        event -> EventUtils.jsonToEvent(new String(event, Charset.defaultCharset()))
                );

        // calculate statistics

        // compute the count of events by node
        eventsByNodeSummation(events);
        // compute the moving average of values by node
        eventsByNodeValueAverage(events);
        // compute the count of events by order
        eventsByOrderSummation(events);
        // compute the moving average of values by order
        eventsByOrderValueAverage(events);
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

    public void eventsByNodeValueAverage(final JavaDStream<Event> events) {
        final JavaPairDStream<Integer, Tuple2<Double, Long>> eventsByNode = events
                .mapToPair(event -> Tuple2.apply(event.getNodeId(), new Tuple2<>(event.getValue(), 1L)))
                .reduceByKey(SUM_AND_COUNT_REDUCER)
                .updateStateByKey(COMPUTE_RUNNING_AVG);

        eventsByNode.foreachRDD(rdd -> {
            LOGGER.info("node average value: {}", rdd.take(100));
            System.out.println("node average value: " + rdd.take(100));
            return null;
        });
    }

    public void eventsByOrderValueAverage(final JavaDStream<Event> events) {
        final JavaPairDStream<Integer, Tuple2<Double, Long>> eventsByNode = events
                .mapToPair(event -> Tuple2.apply(event.getOrderId(), new Tuple2<>(event.getValue(), 1L)))
                .reduceByKey(SUM_AND_COUNT_REDUCER)
                .updateStateByKey(COMPUTE_RUNNING_AVG);

        eventsByNode.foreachRDD(rdd -> {
            LOGGER.info("order average value: {}", rdd.take(100));
            System.out.println("order average value: " + rdd.take(100));
            return null;
        });
    }
}


