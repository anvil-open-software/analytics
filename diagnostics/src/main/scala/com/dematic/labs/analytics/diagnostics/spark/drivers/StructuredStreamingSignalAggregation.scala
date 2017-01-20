package com.dematic.labs.analytics.diagnostics.spark.drivers

import com.datastax.spark.connector.cql.CassandraConnector
import com.dematic.labs.analytics.common.cassandra.Connections
import com.dematic.labs.analytics.common.spark.CassandraDriverConfig.{AUTH_PASSWORD_PROP, AUTH_USERNAME_PROP, CONNECTION_HOST_PROP, KEEP_ALIVE_PROP}
import com.dematic.labs.analytics.common.spark.DriverConsts.SPARK_CHECKPOINT_DIR
import com.dematic.labs.toolkit.helpers.bigdata.communication.{Signal, SignalUtils, SignalValidation}
import org.apache.parquet.Strings
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.streaming.OutputMode.Complete


//todo: still needs to be worked on
object StructuredStreamingSignalAggregation {
  private val APP_NAME = "SS_Signal_Count"

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: StructuredStreamingSignalCount <brokerBootstrapServers> <topic> <cassandraHost> " +
        "<cassandraKeySpace> <optional masterUrl>")
      System.exit(1)
    }

    // set parameters
    val Array(brokers, topics, cassandraHost, cassandraKeyspace, masterUrl) = args
    // all have to be set or todo: will throw an exception
    val cassandraUserName = sys.props(AUTH_USERNAME_PROP)
    val cassandraPassword = sys.props(AUTH_PASSWORD_PROP)
    val keepAliveInMs = sys.props(KEEP_ALIVE_PROP)
    val checkpointDir = sys.props(SPARK_CHECKPOINT_DIR)

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
      .load

    // kafka schema is the following: input columns: [value, timestamp, timestampType, partition, key, topic, offset]

    // 1) query streaming data total counts per topic
    val totalSignalCount = kafka.groupBy("topic").count

    //totalSignalCount.show()
    /* // write the output
     val q1 = totalSignalCount.writeStream
       .option("checkpointLocation", checkpointDir)
       .queryName("total count")
       .format("console")
       .outputMode(Complete)
       .start
     q1.awaitTermination()*/

    // 2) query streaming data group by opcTagId per hour
    val signals = kafka.selectExpr("CAST(value AS STRING)").as(Encoders.STRING)

    // explicitly define signal encoders
    import org.apache.spark.sql.Encoders
    implicit val encoder = Encoders.bean[Signal](classOf[Signal])

    val signalsPerHour = signals.map(SignalUtils.jsonToSignal)

    // 1) query streaming data count by topic


    import spark.implicits._
    var counts = signalsPerHour.groupBy(signalsPerHour.col("timestamp")).count

    // import spark.implicits._


    /**
      * val tumblingWindowDS = stocks2016
      .groupBy(window(stocks2016.col("Date"),"1 week"))
      .agg(avg("Close").as("weekly_average"))
      */

    /* todo: good val counts = signalsPerHour
       .groupBy(window(signalsPerHour.col("timestamp"), "10 minutes", "5 minutes"), signalsPerHour.col("opcTagId"))
       .count
       .orderBy("window")*/



    /*
    val windowedCounts = words.groupBy(
  window($"timestamp", "10 minutes", "5 minutes"),
  $"word"
).count()

     */
    /* val counts = signalsPerHour
        // .withWatermark("timestamp", "10 min")
       .groupBy("timestamp")
       .count*/

    // write the output
    val query = counts.writeStream
      .option("checkpointLocation", checkpointDir)
      .queryName("agg count")
      .format("console")
      .outputMode(Complete)
      .start

    // 2) query streaming data group by opcTagId per hour

    query.awaitTermination
  }

}
