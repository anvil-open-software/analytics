package com.dematic.labs.analytics.ingestion.spark.drivers.diagnostics.kafka;

import com.dematic.labs.analytics.common.spark.DriverConsts;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class SimpleKafkaStructuredStreamingLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaStructuredStreamingLogger.class);
    private static final String APP_NAME = "SS_DIAGNOSTIC_LOGGER";

    public static void main(final String[] args) throws StreamingQueryException {
        // master url is only set for testing or running locally
        if (args.length < 2) {
            throw new IllegalArgumentException("Driver requires Kafka Server Bootstrap, Kafka topics");
        }

        final String kafkaServerBootstrap = args[0];
        final String kafkaTopics = args[1];

        // create the spark session
        final SparkSession.Builder builder = SparkSession.builder();

        builder.appName(APP_NAME);
        final SparkSession spark = builder.getOrCreate();

        // read from the kafka steam
        final Dataset<Row> kafka = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServerBootstrap)
                .option("subscribe", kafkaTopics)
                .option("startingOffsets", "earliest")
                .load();

        // sink the stream to cassandra
        final StreamingQuery start = kafka.writeStream()
                .option("checkpointLocation", System.getProperty(DriverConsts.SPARK_CHECKPOINT_DIR))
                .foreach(new ForeachWriter<Row>() {

                    @Override
                    public void process(final Row row) {
                        // kafka schema is the following: input columns: [value, timestamp, timestampType, partition,
                        // key, topic, offset]
                        final byte[] signal = row.getAs("value");
                        if (signal.length > 0) {
                            LOGGER.trace(signal.toString());
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


                })
                .start();

        start.awaitTermination();
    }


}


