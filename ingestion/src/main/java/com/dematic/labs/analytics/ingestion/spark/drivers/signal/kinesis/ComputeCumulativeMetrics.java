package com.dematic.labs.analytics.ingestion.spark.drivers.signal.kinesis;

import com.dematic.labs.analytics.common.spark.CassandraDriverConfig;
import com.dematic.labs.analytics.common.spark.KinesisStreamConfig;
import com.dematic.labs.analytics.common.spark.StreamConfig;
import com.dematic.labs.analytics.common.spark.StreamFunctions;
import com.dematic.labs.analytics.ingestion.spark.drivers.signal.AggregationFunctions.ComputeMovingSignalAggregation;
import com.dematic.labs.analytics.ingestion.spark.drivers.signal.SignalAggregation;
import com.dematic.labs.toolkit.GenericBuilder;
import com.dematic.labs.toolkit.communication.Signal;
import com.dematic.labs.toolkit.communication.SignalUtils;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

public final class ComputeCumulativeMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(ComputeCumulativeMetrics.class);
    public static final String COMPUTE_METRICS_APP_NAME = "CUMULATIVE_METRICS_GRAINGER_LT";

    // signal stream compute metrics function
    private static final class ComputeMovingMetricsFunction implements VoidFunction<JavaDStream<byte[]>> {
        private final CassandraDriverConfig driverConfig;

        ComputeMovingMetricsFunction(final CassandraDriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(final JavaDStream<byte[]> javaDStream) throws Exception {
            // 1) transform the byte[] to signals
            final JavaDStream<Signal> signals = javaDStream.map(SignalUtils::jsonByteArrayToSignal);
            // 2) compute metrics from signals
            // -- group by opcTagId
            final JavaPairDStream<String, List<Signal>> signalsToOpcTagId =
                    signals.mapToPair(signal -> Tuple2.apply(signal.getOpcTagId(), Collections.singletonList(signal)));
            // -- reduce all signals to opcTagId
            final JavaPairDStream<String, List<Signal>> reduceByKey =
                    signalsToOpcTagId.reduceByKey((signal1, signal2) -> Stream.of(signal1, signal2)
                            .flatMap(Collection::stream).collect(Collectors.toList()));
            // -- compute metric aggregations and save
            final JavaMapWithStateDStream<String, List<Signal>, SignalAggregation, SignalAggregation>
                    mapWithStateDStream = reduceByKey.mapWithState(StateSpec.function(
                    new ComputeMovingSignalAggregation(driverConfig)).
                    // default timeout in seconds
                            timeout(Durations.seconds(60L)));
            // 3) save metrics by bucket to cassandra
            mapWithStateDStream.foreachRDD(rdd -> {
                rdd.foreachPartition(partition -> {
                    final List<SignalAggregation> collect =
                            stream(spliteratorUnknownSize(partition, Spliterator.CONCURRENT), true).
                                    collect(Collectors.toList());
                    if (!collect.isEmpty()) {
                        collect.forEach(signalAggregation -> LOGGER.info("CMM: {}", signalAggregation));
                    }
                });
            });
        }
    }

    public static void main(final String[] args) {
        // master url is only set for testing or running locally
        if (args.length < 3) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint, Kinesis StreamName, " +
                    "CassandraHost, KeySpace, optional driver MasterUrl, driver PollTime");
        }
        final String kinesisEndpoint = args[0];
        final String kinesisStreamName = args[1];
        final String masterUrl;
        final String host;
        final String keySpace;
        final String pollTime;

        //noinspection Duplicates
        if (args.length == 6) {
            host = args[2];
            keySpace = args[3];
            masterUrl = args[4];
            pollTime = args[5];
        } else {
            // no master url
            masterUrl = null;
            host = args[2];
            keySpace = args[3];
            pollTime = args[4];
        }

        // create the driver configuration and checkpoint dir
        final CassandraDriverConfig driverConfig = configure(String.format("%s_%s", keySpace, COMPUTE_METRICS_APP_NAME),
                kinesisEndpoint, kinesisStreamName, host, keySpace, masterUrl, pollTime);

        driverConfig.setCheckPointDirectoryFromSystemProperties(true);

        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext = JavaStreamingContext.getOrCreate(driverConfig.getCheckPointDir(),
                new StreamFunctions.CreateKinesisCassandraStreamingContext(driverConfig,
                        new ComputeMovingMetricsFunction(driverConfig)));
        // todo: creates the table in cassandra to store metrics
        // Start the streaming context and await termination
        LOGGER.info("CMM: starting Compute Moving Metrics Driver with master URL >{}<",
                streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("CMM: spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }

    private static CassandraDriverConfig configure(final String appName, final String kinesisEndpoint,
                                                   final String kinesisStreamName, final String host,
                                                   final String keySpace, final String masterUrl,
                                                   final String pollTime) {

        final StreamConfig kinesisStreamConfig = GenericBuilder.of(KinesisStreamConfig::new).
                with(KinesisStreamConfig::setStreamEndpoint, kinesisEndpoint).
                with(KinesisStreamConfig::setStreamName, kinesisStreamName).
                build();

        return GenericBuilder.of(CassandraDriverConfig::new)
                .with(CassandraDriverConfig::setAppName, appName)
                .with(CassandraDriverConfig::setHost, host)
                .with(CassandraDriverConfig::setKeySpace, keySpace)
                .with(CassandraDriverConfig::setMasterUrl, masterUrl)
                .with(CassandraDriverConfig::setPollTime, pollTime)
                .with(CassandraDriverConfig::setStreamConfig, kinesisStreamConfig)
                .build();
    }
}
