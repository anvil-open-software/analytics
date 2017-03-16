package com.dematic.labs.analytics.diagnostics.spark.drivers

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.spark.connector.cql.CassandraConnector
import com.dematic.labs.analytics.common.cassandra.Connections
import com.dematic.labs.analytics.common.spark.CassandraDriverConfig._
import com.dematic.labs.analytics.common.spark.DriverConsts
import com.dematic.labs.analytics.common.spark.DriverConsts._
import com.dematic.labs.analytics.common.spark.KafkaStreamConfig._
import com.dematic.labs.analytics.diagnostics.spark.drivers.PropertiesUtils.getOrThrow
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
  * -Dspark.streaming.receiver.writeAheadLog.enable=true
  */
object StructuredStreamingSignalCount {
  private val APP_NAME = "SS_Signal_Count"

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: StructuredStreamingSignalCount <brokerBootstrapServers> <topic> <cassandraHost> " +
        "<cassandraKeyspace> <optional masterUrl>")
      System.exit(1)
    }

    val brokers = args(0)
    val topics = args(1)
    val cassandraHost = args(2)
    val cassandraKeyspace = args(3)
    val masterUrl = if (args.length == 5) args(4) else null

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
    builder.config(SPARK_STREAMING_CHECKPOINT_DIR, checkpointDir)
    val spark: SparkSession = builder.getOrCreate

    // create the cassandra table
    Connections.createTable(SignalValidation.createSSTableCql(cassandraKeyspace),
      CassandraConnector.apply(spark.sparkContext.getConf))

    // add query statistic listener to enable monitoring of queries
    if (sys.props.contains(DriverConsts.SPARK_QUERY_STATISTICS)) {
      spark.streams.addListener(new CassandraStreamingQueryListener(APP_NAME, cassandraKeyspace,
        spark.sparkContext.getConf))
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


/**
  *
  *

val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.102.20.11,10.102.20.12")
      .option("subscribe", "mm_test20")
      .option("startingOffsets", "earliest")
      .load

   val totalSignalCount = kafka.groupBy("topic").count

val query = signals.writeStream
      .option("checkpointLocation", checkpointDir)
      .queryName("signal count")
      .format("console")
     // .outputMode(Complete)
      .start

    query.awaitTermination


  org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0

  import org.apache.spark.sql._

val spark = SparkSession
   .builder()
   .appName("SS_test")
   .getOrCreate()

spark.sparkContext.setLogLevel("DEBUG")




  import org.apache.spark.sql._
import org.apache.spark.sql.streaming.OutputMode.Complete

val spark = SparkSession
   .builder()
   .appName("SS_test")
   .getOrCreate()

spark.sparkContext.setLogLevel("DEBUG")









  import org.apache.spark.sql._
import org.apache.spark.sql.streaming.OutputMode.Complete

val spark = SparkSession
   .builder()
   .appName("SS_test")
   .getOrCreate()

spark.sparkContext.setLogLevel("DEBUG")

val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.207.220.11:9092")
      .option("subscribe", "mm_test20")
      .option("startingOffsets", "earliest")
      .load

val totalSignalCount = kafka.groupBy("topic").count

val query = totalSignalCount.writeStream
    //  .option("checkpointLocation", "hdfs:///user/spark/ss_test")
      .queryName("signal count")
      .format("console")
      .outputMode(Complete)
      .start

query.awaitTermination
  */