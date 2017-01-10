package com.dematic.labs.analytics.store.spark.drivers.signal.kafka;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.dematic.labs.analytics.common.cassandra.Connections;
import com.dematic.labs.analytics.common.spark.CassandraDriverConfig;
import com.dematic.labs.analytics.common.spark.KafkaStreamConfig;
import com.dematic.labs.analytics.common.spark.StreamConfig;
import com.dematic.labs.toolkit.helpers.bigdata.communication.Signal;
import com.dematic.labs.toolkit.helpers.bigdata.communication.SignalUtils;
import com.dematic.labs.toolkit.helpers.common.GenericBuilder;
import org.apache.parquet.Strings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static com.dematic.labs.toolkit.helpers.bigdata.communication.Signal.TABLE_NAME;

public final class StructuredStreamingCassandraPersister {
    private static final Logger LOGGER = LoggerFactory.getLogger(StructuredStreamingCassandraPersister.class);
    private static final String APP_NAME = "SS_CASSANDRA_PERSISTER";

    public static void main(final String[] args) throws StreamingQueryException {
        // master url is only set for testing or running locally
        if (args.length < 3) {
            throw new IllegalArgumentException("Driver requires Kafka Server Bootstrap, Kafka topics," +
                    "CassandraHost, KeySpace, optional driver MasterUrl");
        }

        final String kafkaServerBootstrap = args[0];
        final String kafkaTopics = args[1];
        final String masterUrl;
        final String host;
        final String keySpace;

        //noinspection Duplicates
        if (args.length == 5) {
            host = args[2];
            keySpace = args[3];
            masterUrl = args[4];
        } else {
            host = args[2];
            keySpace = args[3];
            // no master url
            masterUrl = null;
        }

        // create the driver configuration
        final CassandraDriverConfig driverConfig = configure(String.format("%s_%s", keySpace, APP_NAME),
                kafkaServerBootstrap, kafkaTopics, host, keySpace, masterUrl);
        // ensure the checkpoint dir is set
        driverConfig.setCheckPointDirectoryFromSystemProperties(true);

        // create the spark session
        final SparkSession.Builder builder = SparkSession.builder();
        if (!Strings.isNullOrEmpty(driverConfig.getMasterUrl())) {
            builder.master(driverConfig.getMasterUrl());
        }
        builder.appName(driverConfig.getAppName());
        builder.config(CassandraDriverConfig.CONNECTION_HOST_PROP, driverConfig.getHost());
        final SparkSession spark = builder.getOrCreate();

        // creates the table in cassandra to store raw signals
        Connections.createTable(Signal.createTableCql(driverConfig.getKeySpace()),
                CassandraConnector.apply(spark.sparkContext().getConf()));

        // read from the kafka steam
        final Dataset<Row> kafka = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", driverConfig.getStreamConfig().getStreamEndpoint())
                .option("subscribe", Strings.join(driverConfig.getStreamConfig().getTopics(), ","))
                .option("startingOffsets", "earliest")
                .load();

        // sink the stream to cassandra
        final StreamingQuery start = kafka.writeStream()
                .option("checkpointLocation", driverConfig.getCheckPointDir())
                .foreach(new ForeachWriter<Row>() {
                    private final CassandraConnector cassandraConnector =
                            CassandraConnector.apply(spark.sparkContext().getConf());

                    @Override
                    public void process(final Row row) {
                        // kafka schema is the following: input columns: [value, timestamp, timestampType, partition,
                        // key, topic, offset]
                        final byte[] signal = row.getAs("value");
                        if (signal.length > 0) {
                            saveToCassandra(signal);
                        } else {
                            LOGGER.error("SSCP: Unexpected Error: Kafka does not contain a 'value' {}", row.toString());
                        }
                    }

                    @Override
                    public void close(final Throwable errorOrNull) {
                        if (errorOrNull != null) {
                            LOGGER.error("SSCP: Unexpected Error:", errorOrNull);
                        }
                    }

                    @Override
                    public boolean open(final long partitionId, final long version) {
                        // cassandra signal table is idempotent thus, no need to track version
                        return true;
                    }

                    private void saveToCassandra(final byte[] signalInBytes) {
                        try {
                            final Signal signal = SignalUtils.jsonByteArrayToSignal(signalInBytes);
                            final Insert stmt = QueryBuilder.insertInto(driverConfig.getKeySpace(), TABLE_NAME)
                                    .value("day", Instant.parse(signal.getDay()).truncatedTo(ChronoUnit.DAYS).toString()) // todo: look into
                                    .value("unique_id", signal.getUniqueId())
                                    .value("id", signal.getId())
                                    .value("value", signal.getValue())
                                    .value("timestamp", signal.getTimestamp())
                                    .value("quality", signal.getQuality())
                                    .value("opc_tag_reading_id", signal.getOpcTagReadingId())
                                    .value("opc_tag_id", signal.getOpcTagId())
                                    .value("proxied_type_name", signal.getProxiedTypeName())
                                    .value("extended_properties", signal.getExtendedProperties());
                            Connections.execute(stmt, cassandraConnector);
                        } catch (final Throwable any) {
                            // just wrap and throw
                            throw new IllegalStateException(any);
                        }
                    }
                })
                .start();

        start.awaitTermination();
    }

    private static CassandraDriverConfig configure(final String appName, final String kafkaServerBootstrap,
                                                   final String kafkaTopics, final String host, final String keySpace,
                                                   final String masterUrl) {
        final StreamConfig kafkaStreamConfig = GenericBuilder.of(KafkaStreamConfig::new)
                .with(KafkaStreamConfig::setStreamEndpoint, kafkaServerBootstrap)
                .with(KafkaStreamConfig::setStreamName, kafkaTopics)
                .with(KafkaStreamConfig::setGroupId, appName)
                .build();

        return GenericBuilder.of(CassandraDriverConfig::new)
                .with(CassandraDriverConfig::setAppName, appName)
                .with(CassandraDriverConfig::setHost, host)
                .with(CassandraDriverConfig::setKeySpace, keySpace)
                .with(CassandraDriverConfig::setMasterUrl, masterUrl)
                .with(CassandraDriverConfig::setStreamConfig, kafkaStreamConfig)
                .build();
    }
}


