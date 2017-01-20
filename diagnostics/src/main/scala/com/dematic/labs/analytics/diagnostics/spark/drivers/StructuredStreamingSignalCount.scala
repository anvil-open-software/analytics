package com.dematic.labs.analytics.diagnostics.spark.drivers

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.spark.connector.cql.CassandraConnector
import com.dematic.labs.analytics.common.cassandra.Connections
import com.dematic.labs.analytics.common.spark.CassandraDriverConfig._
import com.dematic.labs.analytics.common.spark.DriverConsts._
import com.dematic.labs.analytics.common.spark.KafkaStreamConfig._
import com.dematic.labs.toolkit.helpers.bigdata.communication.SignalValidation
import com.dematic.labs.toolkit.helpers.bigdata.communication.SignalValidation.SS_TABLE_NAME
import org.apache.parquet.Strings
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.OutputMode.Complete

/**
  * Need to have the following system properties.
  *
  * -Dspark.cassandra.auth.username=username
  * -Dspark.cassandra.auth.password=password
  * -Dspark.cassandra.connection.keep_alive_ms=5000
  * -Dspark.checkpoint.dir=pathOfCheckpointDir
  */
object StructuredStreamingSignalCount {
  private val APP_NAME = "SS_Signal_Count"

  private def getOrThrow(systemPropertyName: String) = {
    val property = sys.props(systemPropertyName)
    if (property == null) throw new IllegalStateException(String.format("'%s' needs to be set", systemPropertyName))
    property
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: StructuredStreamingSignalCount <brokerBootstrapServers> <topic> <cassandraHost> " +
        "<cassandraKeySpace> <optional masterUrl>")
      System.exit(1)
    }

    // set parameters
    val Array(brokers, topics, cassandraHost, cassandraKeyspace, masterUrl) = args
    // all have to be set or throw exception
    getOrThrow(AUTH_USERNAME_PROP)
    getOrThrow(AUTH_PASSWORD_PROP)
    val keepAliveInMs = getOrThrow(KEEP_ALIVE_PROP)
    val checkpointDir = getOrThrow(SPARK_CHECKPOINT_DIR)
    // additional kafka options
    val kafkaOptions = getPrefixedSystemProperties(KAFKA_ADDITIONAL_CONFIG_PREFIX)

    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    if (!Strings.isNullOrEmpty(masterUrl)) {
      builder.master(masterUrl)
    }
    builder.appName(APP_NAME)
    builder.config(CONNECTION_HOST_PROP, cassandraHost)
    builder.config(KEEP_ALIVE_PROP, keepAliveInMs)
    val spark: SparkSession = builder.getOrCreate

    // create the cassandra table
    Connections.createTable(SignalValidation.createSSTableCql(cassandraKeyspace),
      CassandraConnector.apply(spark.sparkContext.getConf))

    // read from the kafka steam
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .option("startingOffsets", "earliest")
      .options(kafkaOptions)
      .load

    // kafka schema is the following: input columns: [value, timestamp, timestampType, partition, key, topic, offset]

    // 1) query streaming data total counts per topic
    val totalSignalCount = kafka.groupBy("topic").count

    // write the output
    val query = totalSignalCount.writeStream
      .option("checkpointLocation", checkpointDir)
      .queryName("signal count")
      .outputMode(Complete)
      .foreach(new ForeachWriter[Row] {
        private val cassandraConnector = CassandraConnector.apply(spark.sparkContext.getConf)

        override def process(value: Row) {
          val update = QueryBuilder.insertInto(cassandraKeyspace, SS_TABLE_NAME)
            .value("spark_count", value.getAs("count"))
            .value("id", APP_NAME)
          Connections.execute(update, cassandraConnector)
        }

        override def close(errorOrNull: Throwable) {}

        override def open(partitionId: Long, version: Long) = true
      })
      .start

    query.awaitTermination
  }
}
