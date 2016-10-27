package com.dematic.labs.analytics.store.spark.drivers.signal.kafka;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.dematic.labs.analytics.common.cassandra.Connections;
import com.dematic.labs.analytics.common.spark.CassandraDriverConfig;
import com.dematic.labs.analytics.common.spark.KafkaStreamConfig;
import com.dematic.labs.analytics.common.spark.StreamConfig;
import com.dematic.labs.analytics.common.spark.StreamFunctions;
import com.dematic.labs.toolkit.helpers.common.GenericBuilder;
import com.dematic.labs.toolkit.helpers.bigdata.communication.Signal;
import com.dematic.labs.toolkit.helpers.bigdata.communication.SignalUtils;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public final class CassandraPersister {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraPersister.class);
    public static final String APP_NAME = "CASSANDRA_PERSISTER";

    private CassandraPersister() {
    }

    // signal stream processing function
    private static final class PersistFunction implements VoidFunction<JavaDStream<byte[]>> {
        private final CassandraDriverConfig driverConfig;

        PersistFunction(final CassandraDriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(final JavaDStream<byte[]> javaDStream) throws Exception {
            // transform the byte[] (byte arrays are json) to signals
            final JavaDStream<Signal> eventStream = javaDStream.map(SignalUtils::jsonByteArrayToSignal);
            eventStream.foreachRDD(rdd -> {
                javaFunctions(rdd).writerBuilder(driverConfig.getKeySpace(), Signal.TABLE_NAME,
                        mapToRow(Signal.class)).saveToCassandra();
            });
        }
    }

    public static void main(final String[] args) throws InterruptedException {
        // master url is only set for testing or running locally
        if (args.length < 3) {
            throw new IllegalArgumentException("Driver requires Kafka Server Bootstrap, Kafka topics" +
                    "CassandraHost, KeySpace, optional driver MasterUrl, driver PollTime");
        }
        final String kafkaServerBootstrap = args[0];
        final String kafkaTopics = args[1];
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
        final CassandraDriverConfig driverConfig = configure(String.format("%s_%s", keySpace, APP_NAME),
                kafkaServerBootstrap, kafkaTopics, host, keySpace, masterUrl, pollTime);

        driverConfig.setCheckPointDirectoryFromSystemProperties(true);
        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext = JavaStreamingContext.getOrCreate(driverConfig.getCheckPointDir(),
                new StreamFunctions.CreateKafkaCassandraStreamingContext(driverConfig,
                        new PersistFunction(driverConfig)));
        // creates the table in cassandra to store raw signals
        Connections.createTable(Signal.createTableCql(driverConfig.getKeySpace()),
                CassandraConnector.apply(streamingContext.sparkContext().getConf()));

        // Start the streaming context and await termination
        LOGGER.info("KCP: starting Kafka Cassandra Persister Driver with master URL >{}<",
                streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("KCP: spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }

    private static CassandraDriverConfig configure(final String appName, final String kafkaServerBootstrap,
                                                   final String kafkaTopics, final String host, final String keySpace,
                                                   final String masterUrl, final String pollTime) {
        final StreamConfig kafkaStreamConfig = GenericBuilder.of(KafkaStreamConfig::new)
                .with(KafkaStreamConfig::setStreamEndpoint, kafkaServerBootstrap)
                .with(KafkaStreamConfig::setStreamName, kafkaTopics)
                .build();

        return GenericBuilder.of(CassandraDriverConfig::new)
                .with(CassandraDriverConfig::setAppName, appName)
                .with(CassandraDriverConfig::setHost, host)
                .with(CassandraDriverConfig::setKeySpace, keySpace)
                .with(CassandraDriverConfig::setMasterUrl, masterUrl)
                .with(CassandraDriverConfig::setPollTime, pollTime)
                .with(CassandraDriverConfig::setStreamConfig, kafkaStreamConfig)
                .build();
    }
}
