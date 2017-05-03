package com.dematic.labs.analytics.diagnostics.spark.drivers

import com.dematic.labs.analytics.common.spark.DriverConsts._
import com.dematic.labs.analytics.common.spark.KafkaStreamConfig.{KAFKA_ADDITIONAL_CONFIG_PREFIX, getPrefixedSystemProperties}
import com.dematic.labs.analytics.diagnostics.spark.drivers.PropertiesUtils.getOrThrow
import org.apache.parquet.Strings
import org.apache.spark.sql.{Encoders, SparkSession}

/**
  * Driver will save json signals to parquet data storage.
  *
  **/
object StructuredStreamingParquetPersister {
  private val APP_NAME = "SS_Parquet_Persister"

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StructuredStreamingParquetPersister <brokerBootstrapServers> <topic> <optional masterUrl>")
      System.exit(1)
    }

    // set parameters
    val brokers = args(0)
    val topics = args(1)
    val masterUrl = if (args.length == 3) args(2) else null

    // spark system properties
    val checkpointDir = getOrThrow(SPARK_CHECKPOINT_DIR)
    val parquetDir = getOrThrow(SPARK_PARQUET_DIR)
    // kafka options
    val kafkaOptions = getPrefixedSystemProperties(KAFKA_ADDITIONAL_CONFIG_PREFIX)

    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    if (!Strings.isNullOrEmpty(masterUrl)) {
      builder.master(masterUrl)
    }
    builder.appName(APP_NAME)

    builder.config(SPARK_STREAMING_CHECKPOINT_DIR, checkpointDir)
    val spark: SparkSession = builder.getOrCreate

    // read from the kafka steam
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .option("startingOffsets", "earliest")
      .options(kafkaOptions)
      .load

    // kafka schema is the following: input columns: [value, timestamp, timestampType, partition, key, topic, offset]
    val signals = kafka.selectExpr("CAST(value AS STRING)").as(Encoders.STRING)

    // write aggregate sinks to parquet
    val out = signals.writeStream
      .queryName("parquet_persister")
      .format("parquet")
      .option("path", parquetDir)
      .option("checkpointLocation", checkpointDir)
      .start
    out.awaitTermination()
  }
}
