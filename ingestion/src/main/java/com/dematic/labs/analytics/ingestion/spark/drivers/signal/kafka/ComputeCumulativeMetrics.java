/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.ingestion.spark.drivers.signal.kafka;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.dematic.labs.analytics.common.GenericBuilder;
import com.dematic.labs.analytics.common.cassandra.Connections;
import com.dematic.labs.analytics.common.communication.Signal;
import com.dematic.labs.analytics.common.communication.SignalUtils;
import com.dematic.labs.analytics.common.communication.SignalValidation;
import com.dematic.labs.analytics.common.spark.DriverConsts;
import com.dematic.labs.analytics.common.spark.KafkaStreamConfig;
import com.dematic.labs.analytics.common.spark.StreamConfig;
import com.dematic.labs.analytics.common.spark.StreamFunctions;
import com.dematic.labs.analytics.ingestion.spark.drivers.signal.Aggregation;
import com.dematic.labs.analytics.ingestion.spark.drivers.signal.AggregationFunctions;
import com.dematic.labs.analytics.ingestion.spark.drivers.signal.ComputeCumulativeMetricsDriverConfig;
import com.dematic.labs.analytics.ingestion.spark.drivers.signal.SignalAggregation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.dematic.labs.analytics.common.spark.OffsetManager.manageOffsets;
import static com.dematic.labs.analytics.common.spark.OffsetManager.saveOffsetRanges;

public final class ComputeCumulativeMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(ComputeCumulativeMetrics.class);
    private static final String APP_NAME = "CUMULATIVE_SIGNAL_METRICS";

    // just add a flag to be able to turn off and on validation of counts
    private static final boolean VALIDATE_COUNTS = System.getProperty(DriverConsts.SPARK_DRIVER_VALIDATE_COUNTS) != null;

    private ComputeCumulativeMetrics() {
    }

    private static final class ComputeCumulativeSignalMetrics implements VoidFunction<JavaInputDStream<ConsumerRecord<String, byte[]>>> {
        private final ComputeCumulativeMetricsDriverConfig driverConfig;

        ComputeCumulativeSignalMetrics(final ComputeCumulativeMetricsDriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(final JavaInputDStream<ConsumerRecord<String, byte[]>> inputStream) throws Exception {
            // 1) save offsets from the beginning
            if (manageOffsets()) {
                final CassandraConnector cassandraConnector = CassandraConnector.apply(inputStream.context().conf());
                inputStream.foreachRDD(rdd -> {
                    final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    saveOffsetRanges(driverConfig.getKeySpace(), offsetRanges, cassandraConnector);
                });
            }

            // 2) transform the byte[] (byte arrays are json) to signals and save raw signals
            final JavaDStream<Signal> signals =
                    inputStream.map((Function<ConsumerRecord<String, byte[]>, byte[]>) ConsumerRecord::value)
                            .map(SignalUtils::jsonByteArrayToSignal);
            signals.foreachRDD(rdd -> javaFunctions(rdd).writerBuilder(driverConfig.getKeySpace(), Signal.TABLE_NAME,
                    mapToRow(Signal.class)).saveToCassandra());

            // 3) if validation needed, update counts
            if (VALIDATE_COUNTS) {
                // map to signal validation and save to cassandra
                final JavaDStream<SignalValidation> signalValidation =
                        signals.map((Function<Signal, SignalValidation>)
                                signal -> new SignalValidation(driverConfig.getAppName(), 0L, 0L, 1L));
                signalValidation.foreachRDD(rdd -> javaFunctions(rdd).writerBuilder(driverConfig.getKeySpace(),
                        SignalValidation.TABLE_NAME, mapToRow(SignalValidation.class)).saveToCassandra());
            }

            // 4) aggregate by key and aggregation time

            // -- key is by opc tag id and aggregation time
            final JavaPairDStream<Tuple2<Long, Date>, List<Signal>> pairDStream =
                    signals.mapToPair((PairFunction<Signal, Tuple2<Long, Date>, List<Signal>>) signal -> {
                        final Tuple2<Long, Date> key = new Tuple2<>(signal.getOpcTagId(),
                                driverConfig.getAggregateBy().time(signal.getTimestamp()));
                        return new Tuple2<>(key, Collections.singletonList(signal));
                    });

            // -- reduce by opc tag id and aggregation time
            final JavaPairDStream<Tuple2<Long, Date>, List<Signal>> reduceByKey =
                    pairDStream.reduceByKey((signal1, signal2) -> Stream.of(signal1, signal2)
                            .flatMap(Collection::stream).collect(Collectors.toList()));

            // 5) calculate stats
            final JavaMapWithStateDStream<Tuple2<Long, Date>, List<Signal>, SignalAggregation, SignalAggregation>
                    mapWithStateDStream = reduceByKey.mapWithState(StateSpec.function(
                    new AggregationFunctions.ComputeMovingSignalAggregationByOpcTagIdAndAggregation(driverConfig)).
                    // default timeout in seconds
                            timeout(Durations.seconds(60L)));

            // 6) save aggregations and offsets if needed
            mapWithStateDStream.foreachRDD(rdd -> javaFunctions(rdd).writerBuilder(driverConfig.getKeySpace(),
                    SignalAggregation.TABLE_NAME, mapToRow(SignalAggregation.class)).
                    saveToCassandra());
        }
    }

    public static void main(final String[] args) throws InterruptedException {
        // master url is only set for testing or running locally
        if (args.length < 3) {
            throw new IllegalArgumentException("Driver requires Kafka Server Bootstrap, Kafka topics" +
                    "CassandraHost, KeySpace, optional driver MasterUrl, aggregateBy[HOUR,MINUTE], driver PollTime");
        }
        final String kafkaServerBootstrap = args[0];
        final String kafkaTopics = args[1];
        final String masterUrl;
        final String host;
        final String keySpace;
        final Aggregation aggregateBy;
        final String pollTime;

        //noinspection Duplicates
        if (args.length == 7) {
            host = args[2];
            keySpace = args[3];
            masterUrl = args[4];
            aggregateBy = Aggregation.valueOf(args[5]);
            pollTime = args[6];
        } else {
            // no master url
            masterUrl = null;
            host = args[2];
            keySpace = args[3];
            aggregateBy = Aggregation.valueOf(args[4]);
            pollTime = args[5];
        }

        // create the driver configuration and checkpoint dir
        final ComputeCumulativeMetricsDriverConfig driverConfig = configure(String.format("%s_%s", keySpace, APP_NAME),
                kafkaServerBootstrap, kafkaTopics, host, keySpace, masterUrl, aggregateBy, pollTime);

        driverConfig.setCheckPointDirectoryFromSystemProperties(true);
        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext = JavaStreamingContext.getOrCreate(driverConfig.getCheckPointDir(),
                new StreamFunctions.CreateKafkaCassandraStreamingContext(driverConfig,
                        new ComputeCumulativeSignalMetrics(driverConfig)));

        // create the cassandra tables
        Connections.createTable(Signal.createTableCql(driverConfig.getKeySpace()),
                CassandraConnector.apply(streamingContext.sparkContext().getConf()));
        Connections.createTable(SignalAggregation.createTableCql(driverConfig.getKeySpace()),
                CassandraConnector.apply(streamingContext.sparkContext().getConf()));

        if (VALIDATE_COUNTS) {
            // create the count table
            Connections.createTable(SignalValidation.createCounterTableCql(driverConfig.getKeySpace()),
                    CassandraConnector.apply(streamingContext.sparkContext().getConf()));
        }

        // Start the streaming context and await termination
        LOGGER.info("SM: starting Compute Cumulative Signal Metrics Driver with master URL >{}<",
                streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("SM: spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }

    private static ComputeCumulativeMetricsDriverConfig configure(final String appName,
                                                                  final String kafkaServerBootstrap,
                                                                  final String kafkaTopics, final String host,
                                                                  final String keySpace, final String masterUrl,
                                                                  final Aggregation aggregationBy,
                                                                  final String pollTime) {

        final StreamConfig kafkaStreamConfig = GenericBuilder.of(KafkaStreamConfig::new)
                .with(KafkaStreamConfig::setStreamEndpoint, kafkaServerBootstrap)
                .with(KafkaStreamConfig::setStreamName, kafkaTopics)
                .with(KafkaStreamConfig::setGroupId, appName)
                .build();

        return GenericBuilder.of(ComputeCumulativeMetricsDriverConfig::new)
                .with(ComputeCumulativeMetricsDriverConfig::setAppName, appName)
                .with(ComputeCumulativeMetricsDriverConfig::setHost, host)
                .with(ComputeCumulativeMetricsDriverConfig::setKeySpace, keySpace)
                .with(ComputeCumulativeMetricsDriverConfig::setKeepAlive, keepAlive(pollTime))
                .with(ComputeCumulativeMetricsDriverConfig::setMasterUrl, masterUrl)
                .with(ComputeCumulativeMetricsDriverConfig::setAggregateBy, aggregationBy)
                .with(ComputeCumulativeMetricsDriverConfig::setPollTime, pollTime)
                .with(ComputeCumulativeMetricsDriverConfig::setStreamConfig, kafkaStreamConfig)
                .build();
    }

    private static String keepAlive(final String pollTime) {
        // keep alive is pollTime + 5
        return String.valueOf(Integer.valueOf(pollTime) + 5 * 1000);
    }
}
