package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.dematic.labs.analytics.common.sparks.DriverConfig;
import com.dematic.labs.toolkit.communication.Event;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.dematic.labs.analytics.common.sparks.DriverUtils.getKinesisCheckpointWindow;
import static com.dematic.labs.toolkit.aws.Connections.getNumberOfShards;

public final class Functions implements Serializable {
    private Functions() {
    }

    // Lambda Functions
    static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

    // Overridden Functions
    static final class AggregateEventToBucketFunction implements PairFunction<Event, String, Long> {
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

    static final class CreateDStreamFunction implements Function0<JavaDStream<byte[]>> {
        private final DriverConfig driverConfig;
        private final JavaStreamingContext streamingContext;

        public CreateDStreamFunction(final DriverConfig driverConfig, final JavaStreamingContext streamingContext) {
            this.driverConfig = driverConfig;
            this.streamingContext = streamingContext;
        }

        @Override
        public JavaDStream<byte[]> call() throws Exception {
            final String kinesisEndpoint = driverConfig.getKinesisEndpoint();
            final String streamName = driverConfig.getStreamName();
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

    static final class CreateStreamingContextFunction implements Function0<JavaStreamingContext> {
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
}
