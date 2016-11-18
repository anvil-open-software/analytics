package com.dematic.labs.analytics.diagnostics.spark.drivers

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies


/**
  *
  * This driver is only for pulling data from the stream and logging partition to output.
  *
  */
object SimpleKafkaLoggingDriver {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: SimpleTestDriver <broker bootstrap servers> <topic> <groupId> <offsetReset>")
      System.exit(1)
    }

    val Array(brokers, topic, groupId, offsetReset) = args
    val preferredHosts = LocationStrategies.PreferConsistent
    val topics = List(topic)

    val kafkaParams = Map(
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> offsetReset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val sparkConf = new SparkConf().setAppName("SimpleTestDriver"+"_" +topic)
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))


    val dstream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    dstream.foreachRDD { rdd =>
      // Get the offset ranges in the RDD and log. Probably should choose a standard logger...
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        println(s"  ${o.topic} partition ${o.partition} offset range: ${o.fromOffset} to ${o.untilOffset}")
      }
    }

    streamingContext.start
    streamingContext.awaitTermination()

  }

}
