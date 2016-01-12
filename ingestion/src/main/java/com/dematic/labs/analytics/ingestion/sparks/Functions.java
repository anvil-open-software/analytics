package com.dematic.labs.analytics.ingestion.sparks;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.dematic.labs.analytics.common.sparks.DriverConfig;
import com.dematic.labs.analytics.ingestion.sparks.drivers.InterArrivalTimeState;
import com.dematic.labs.toolkit.communication.Event;

import java.util.Optional;

import com.google.common.base.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.joda.time.DateTime;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.dematic.labs.analytics.common.sparks.DriverUtils.getKinesisCheckpointWindow;
import static com.dematic.labs.toolkit.aws.Connections.getNumberOfShards;

@SuppressWarnings("unused")
public final class Functions implements Serializable {
    private Functions() {
    }

    // Lambda Functions
    public static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

    public static Function2<List<Long>, Optional<Long>, Optional<Long>> COMPUTE_RUNNING_SUM
            = (nums, existing) -> {
        long sum = existing.orElse(0L);
        for (long i : nums) {
            sum += i;
        }
        return Optional.of(sum);
    };

    public static Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>> SUM_AND_COUNT_REDUCER
            = (x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2());

    public static Function2<List<Tuple2<Double, Long>>, Optional<Tuple2<Double, Long>>, Optional<Tuple2<Double, Long>>>
            COMPUTE_RUNNING_AVG = (sums, existing) -> {
        Tuple2<Double, Long> avgAndCount = existing.orElse(new Tuple2<>(0.0, 0L));

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

    // Overridden Functions

    // creation functions
    public static final class CreateDStreamFunction implements Function0<JavaDStream<byte[]>> {
        private final DriverConfig driverConfig;
        private final JavaStreamingContext streamingContext;

        public CreateDStreamFunction(final DriverConfig driverConfig, final JavaStreamingContext streamingContext) {
            this.driverConfig = driverConfig;
            this.streamingContext = streamingContext;
        }

        @Override
        public JavaDStream<byte[]> call() throws Exception {
            final String kinesisEndpoint = driverConfig.getKinesisEndpoint();
            final String streamName = driverConfig.getKinesisStreamName();
            // create the dstream
            final int shards = getNumberOfShards(kinesisEndpoint, streamName);
            // create 1 Kinesis Worker/Receiver/DStream for each shard
            final List<JavaDStream<byte[]>> streamsList = new ArrayList<>(shards);
            for (int i = 0; i < shards; i++) {
                streamsList.add(
                        KinesisUtils.createStream(streamingContext, streamName, kinesisEndpoint,
                                getKinesisCheckpointWindow(), InitialPositionInStream.TRIM_HORIZON,
                                StorageLevel.MEMORY_ONLY())
                );
            }
            // Union all the streams if there is more than 1 stream
            final JavaDStream<byte[]> unionStreams;
            if (streamsList.size() > 1) {
                unionStreams = streamingContext.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
            } else {
                unionStreams = streamsList.get(0);
            }
            return unionStreams;
        }
    }

    public static final class CreateStreamingContextFunction implements Function0<JavaStreamingContext> {
        private final DriverConfig driverConfig;
        private final VoidFunction<JavaDStream<byte[]>> eventStreamProcessor;

        public CreateStreamingContextFunction(final DriverConfig driverConfig,
                                              final VoidFunction<JavaDStream<byte[]>> eventStreamProcessor) {
            this.driverConfig = driverConfig;
            this.eventStreamProcessor = eventStreamProcessor;
        }

        @Override
        public JavaStreamingContext call() throws Exception {
            // create spark configure
            final SparkConf sparkConfiguration = new SparkConf().setAppName(driverConfig.getAppName());
            // if master url set, apply
            if (!Strings.isNullOrEmpty(driverConfig.getMasterUrl())) {
                sparkConfiguration.setMaster(driverConfig.getMasterUrl());
            }
            // create the streaming context
            final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConfiguration,
                    driverConfig.getPollTime());
            // create the dstream
            final JavaDStream<byte[]> dStream =
                    new CreateDStreamFunction(driverConfig, streamingContext).call();
            // work on the streams
            eventStreamProcessor.call(dStream);
            // set the checkpoint dir
            streamingContext.checkpoint(driverConfig.getCheckPointDir());
            // return the streaming context
            return streamingContext;
        }
    }

    // driver functions
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


    public static final class StatefulEventByNodeFunction implements Function4<Time, String,
            com.google.common.base.Optional<List<Event>>, State<InterArrivalTimeState>,
            com.google.common.base.Optional<String>> {

        @Override
        public com.google.common.base.Optional<String> call(final Time time, final String nodeId,
                                                            final com.google.common.base.Optional<List<Event>> events,
                                                            final State<InterArrivalTimeState> state) throws Exception {

            final InterArrivalTimeState interArrivalTimeState;
            if (state.exists()) {
                // get existing events
                interArrivalTimeState = state.get();
                // determine if we should remove state
                if (state.isTimingOut()) {
                    System.out.println();
                 //   state.remove();
                } else {
                    // add new events to state
                    interArrivalTimeState.addNewEvents(events.get());
                    interArrivalTimeState.updateBuffer(time.milliseconds());
                    state.update(interArrivalTimeState);
                }
            } else {
                // add the initial state, todo: make duration configurable
                interArrivalTimeState = new InterArrivalTimeState(time.milliseconds(), 20L, events.get());
                state.update(interArrivalTimeState);
            }

            // todo: figure out the return type
            return com.google.common.base.Optional.of(nodeId);
        }
    }

}
