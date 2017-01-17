package com.dematic.labs.analytics.diagnostics.spark.drivers

import com.datastax.spark.connector.cql.CassandraConnector
import com.dematic.labs.analytics.common.cassandra.Connections
import com.dematic.labs.analytics.common.spark.CassandraDriverConfig._
import com.dematic.labs.analytics.common.spark.DriverConsts._
import com.dematic.labs.toolkit.helpers.bigdata.communication.SignalValidation
import org.apache.parquet.Strings
import org.apache.spark.sql.streaming.OutputMode.Complete
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object StructuredStreamingSignalCount {
  private val APP_NAME = "SS_Signal_Count"

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: StructuredStreamingSignalCount <brokerBootstrapServers> <topic> <cassandraHost> " +
        "<cassandraKeySpace> <optional masterUrl>")
      System.exit(1)
    }

    // set parameters
    val Array(brokers, topics, cassandraHost, cassandraKeyspace, masterUrl) = args
    // all have to be set or will throw an exception
    val cassandraUserName = sys.env(AUTH_USERNAME_PROP)
    val cassandraPassword = sys.env(AUTH_PASSWORD_PROP)
    val keepAliveInMs = sys.env(KEEP_ALIVE_PROP)
    val checkpointDir = sys.env(SPARK_CHECKPOINT_DIR)

    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    if (!Strings.isNullOrEmpty(masterUrl)) {
      builder.master(masterUrl)
    }
    builder.appName(APP_NAME)
    builder.config(CONNECTION_HOST_PROP, cassandraHost)
    builder.config(KEEP_ALIVE_PROP, keepAliveInMs.*(1000))
    val spark: SparkSession = builder.getOrCreate

    // create the cassandra table
    Connections.createTable(SignalValidation.createTableCql(cassandraKeyspace),
      CassandraConnector.apply(spark.sparkContext.getConf))

    // read from the kafka steam
    val kafka: Dataset[Row] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .option("startingOffsets", "earliest")
      .load

    // query streaming data
    // kafka schema is the following: input columns: [value, timestamp, timestampType, partition, key, topic, offset]
    val counts = kafka.groupBy("topic").count

    // write the output
    val query = counts.writeStream
      .format("console")
      .outputMode(Complete)
      .start

    query.awaitTermination
  }
}
