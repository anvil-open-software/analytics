/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.diagnostics.spark.drivers

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.spark.connector.cql.CassandraConnector
import com.dematic.labs.analytics.common.cassandra.Connections
import com.dematic.labs.analytics.common.communication.{Signal, SignalUtils}
import com.dematic.labs.analytics.common.spark.CassandraDriverConfig.{AUTH_PASSWORD_PROP, AUTH_USERNAME_PROP, CONNECTION_HOST_PROP, KEEP_ALIVE_PROP}
import com.dematic.labs.analytics.common.spark.DriverConsts
import com.dematic.labs.analytics.common.spark.DriverConsts._
import com.dematic.labs.analytics.common.spark.KafkaStreamConfig.{KAFKA_ADDITIONAL_CONFIG_PREFIX, getPrefixedSystemProperties}
import com.dematic.labs.analytics.diagnostics.spark.drivers.PropertiesUtils.getOrThrow
import com.dematic.labs.analytics.monitor.spark.{MonitorConsts, PrometheusStreamingQueryListener}
import org.apache.parquet.Strings
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{window, _}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Encoders, _}

object StructuredStreamingSignalAggregation {
  private val APP_NAME = "SS_Signal_Aggregation"
  private val TABLE_NAME = "ss_signal_aggregation"

  private def createTableCql(keyspace: String): String = {
    String.format("CREATE TABLE if not exists %s.%s (" +
      " opc_tag_id bigint," +
      " aggregate timestamp," +
      " count bigint," +
      " sum bigint," +
      " min bigint," +
      " max bigint," +
      " avg double," +
      " PRIMARY KEY ((opc_tag_id), aggregate))" +
      " WITH CLUSTERING ORDER BY (aggregate DESC);", keyspace, TABLE_NAME)
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: StructuredStreamingSignalCount <brokerBootstrapServers> <topic> <cassandraHost> " +
        "<cassandraKeySpace> <optional masterUrl>")
      System.exit(1)
    }

    // set parameters
    val brokers = args(0)
    val topics = args(1)
    val cassandraHost = args(2)
    val cassandraKeyspace = args(3)
    val masterUrl = if (args.length == 5) args(4) else null

    // cassandra system properties
    getOrThrow(AUTH_USERNAME_PROP)
    getOrThrow(AUTH_PASSWORD_PROP)
    val keepAliveInMs = getOrThrow(KEEP_ALIVE_PROP)
    // spark system properties
    val sqlPartitions = sys.props(SPARK_SQL_SHUFFLE_PARTITIONS)
    val queryTriggerProp = sys.props(SPARK_QUERY_TRIGGER)
    // '0' indicates the query will run as fast as possible
    val queryTrigger = if (!Strings.isNullOrEmpty(queryTriggerProp)) queryTriggerProp else "0 seconds"
    val outputModeProp = sys.props(SPARK_OUTPUT_MODE)
    val outputMode = if (!Strings.isNullOrEmpty(outputModeProp)) outputModeProp else "Append"
    val watermarkTime = getOrThrow(SPARK_WATERMARK_TIME)
    val checkpointDir = getOrThrow(SPARK_CHECKPOINT_DIR)
    // kafka options
    val kafkaOptions = getPrefixedSystemProperties(KAFKA_ADDITIONAL_CONFIG_PREFIX)

    // create the spark session
    val builder: SparkSession.Builder = SparkSession.builder
    if (!Strings.isNullOrEmpty(masterUrl)) {
      builder.master(masterUrl)
    }
    builder.appName(APP_NAME)
    builder.config(CONNECTION_HOST_PROP, cassandraHost)
    builder.config(KEEP_ALIVE_PROP, keepAliveInMs)
    builder.config(SPARK_STREAMING_CHECKPOINT_DIR, checkpointDir)
    val spark: SparkSession = builder.getOrCreate
    // set sql partitions if set
    if (!Strings.isNullOrEmpty(sqlPartitions)) spark.sql("SET spark.sql.shuffle.partitions=" + sqlPartitions)

    // spark.sparkContext.setLogLevel("DEBUG")

    // create the aggregation table
    Connections.createTable(createTableCql(cassandraKeyspace),
      CassandraConnector.apply(spark.sparkContext.getConf))

    // add query statistic listener to enable monitoring of queries
    if (sys.props.contains(DriverConsts.SPARK_QUERY_STATISTICS)) {
      spark.streams.addListener(new CassandraStreamingQueryListener(APP_NAME, cassandraKeyspace,
        spark.sparkContext.getConf))
    }

    if (sys.props.contains(MonitorConsts.SPARK_QUERY_MONITOR_PUSH_GATEWAY)) {
      spark.streams.addListener(new PrometheusStreamingQueryListener(spark.sparkContext.getConf, APP_NAME))
    }

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

    // explicitly define signal encoders
    implicit val encoder: Encoder[Signal] = Encoders.bean[Signal](classOf[Signal])
    // map json signal to signal object
    val signalsPerHour = signals.map(SignalUtils.jsonToSignal)

    import spark.implicits._

    // aggregate by opcTagId and time and watermark data for 24 hours
    val aggregate = signalsPerHour
      .withWatermark("timestamp", watermarkTime)
      .groupBy(window($"timestamp", "5 minutes") as 'aggregate_time, $"opcTagId")
      .agg(count($"opcTagId"), avg($"value"), min($"value"), max($"value"), sum($"value"))

    // write aggregate sinks to cassandra
    val query = aggregate.writeStream
      .trigger(Trigger.ProcessingTime(queryTrigger))
      .option("checkpointLocation", checkpointDir)
      .queryName("aggregate over time")
      .outputMode(outputMode)
      .foreach(new ForeachWriter[Row] {
        private val cassandraConnector = CassandraConnector.apply(spark.sparkContext.getConf)

        override def open(partitionId: Long, version: Long) = true

        override def process(row: Row) {
          val aggregateTime = row.getAs(0).asInstanceOf[GenericRowWithSchema]
          val update = QueryBuilder.update(cassandraKeyspace, TABLE_NAME)
            .`with`(QueryBuilder.set("count", row.getAs(2)))
            .and(QueryBuilder.set("avg", row.getAs(3)))
            .and(QueryBuilder.set("min", row.getAs(4)))
            .and(QueryBuilder.set("max", row.getAs(5)))
            .and(QueryBuilder.set("sum", row.getAs(6)))
            .where(QueryBuilder.eq("opc_tag_id", row.getAs(1)))
            .and(QueryBuilder.eq("aggregate", aggregateTime.get(0)))
          Connections.execute(update, cassandraConnector)
        }

        override def close(errorOrNull: Throwable) {}
      })
      .start
    query.awaitTermination
  }
}
