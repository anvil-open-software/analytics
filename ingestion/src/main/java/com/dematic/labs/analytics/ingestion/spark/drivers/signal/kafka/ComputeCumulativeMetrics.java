package com.dematic.labs.analytics.ingestion.spark.drivers.signal.kafka;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.dematic.labs.analytics.common.cassandra.Connections;
import com.dematic.labs.analytics.common.spark.KafkaStreamConfig;
import com.dematic.labs.analytics.common.spark.StreamConfig;
import com.dematic.labs.analytics.common.spark.StreamFunctions;
import com.dematic.labs.analytics.ingestion.spark.drivers.signal.Aggregation;
import com.dematic.labs.analytics.ingestion.spark.drivers.signal.AggregationFunctions;
import com.dematic.labs.analytics.ingestion.spark.drivers.signal.ComputeCumulativeMetricsDriverConfig;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public final class ComputeCumulativeMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(ComputeCumulativeMetrics.class);
    public static final String APP_NAME = "CUMULATIVE_SIGNAL_METRICS";

    private static final class ComputeCumulativeSignalMetrics implements VoidFunction<JavaDStream<byte[]>> {
        private final ComputeCumulativeMetricsDriverConfig driverConfig;

        ComputeCumulativeSignalMetrics(final ComputeCumulativeMetricsDriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(final JavaDStream<byte[]> javaDStream) throws Exception {
            // 1) transform the byte[] (byte arrays are json) to signals and save raw signals
            final JavaDStream<Signal> signals = javaDStream.map(SignalUtils::jsonByteArrayToSignal);
            CassandraStreamingJavaUtil.javaFunctions(signals).writerBuilder(driverConfig.getKeySpace(),
                    Signal.TABLE_NAME, mapToRow(Signal.class)).saveToCassandra();

            // 2) compute metrics from signals
            // -- group by opcTagId
            final JavaPairDStream<Long, List<Signal>> signalsToOpcTagId =
                    signals.mapToPair(signal -> Tuple2.apply(signal.getOpcTagId(), Collections.singletonList(signal)));

            // -- reduce all signals to opcTagId
            final JavaPairDStream<Long, List<Signal>> reduceByKey =
                    signalsToOpcTagId.reduceByKey((signal1, signal2) -> Stream.of(signal1, signal2)
                            .flatMap(Collection::stream).collect(Collectors.toList()));
            // -- compute metric aggregations and save
            final JavaMapWithStateDStream<Long, List<Signal>, SignalAggregation, SignalAggregation>
                    mapWithStateDStream = reduceByKey.mapWithState(StateSpec.
                    function(new AggregationFunctions.ComputeMovingSignalAggregation(driverConfig)).
                    // default timeout in seconds
                            timeout(Durations.seconds(60L)));

            // 3) save aggregations
            mapWithStateDStream.foreachRDD(rdd -> {
                //todo: is this the best way to do save
                CassandraJavaUtil.javaFunctions(rdd).writerBuilder(driverConfig.getKeySpace(),
                        SignalAggregation.TABLE_NAME, mapToRow(SignalAggregation.class)).
                        saveToCassandra();
            });
        }
    }

    public static void main(final String[] args) {
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
                CassandraConnector.apply(streamingContext.sc().getConf()));
        Connections.createTable(SignalAggregation.createTableCql(driverConfig.getKeySpace()),
                CassandraConnector.apply(streamingContext.sc().getConf()));

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
                .build();

        return GenericBuilder.of(ComputeCumulativeMetricsDriverConfig::new)
                .with(ComputeCumulativeMetricsDriverConfig::setAppName, appName)
                .with(ComputeCumulativeMetricsDriverConfig::setHost, host)
                .with(ComputeCumulativeMetricsDriverConfig::setKeySpace, keySpace)
                .with(ComputeCumulativeMetricsDriverConfig::setMasterUrl, masterUrl)
                .with(ComputeCumulativeMetricsDriverConfig::setAggregateBy, aggregationBy)
                .with(ComputeCumulativeMetricsDriverConfig::setPollTime, pollTime)
                .with(ComputeCumulativeMetricsDriverConfig::setStreamConfig, kafkaStreamConfig)
                .build();
    }
}
