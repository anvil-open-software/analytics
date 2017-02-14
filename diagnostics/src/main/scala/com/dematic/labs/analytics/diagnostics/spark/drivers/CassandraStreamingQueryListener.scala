package com.dematic.labs.analytics.diagnostics.spark.drivers

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.spark.connector.cql.CassandraConnector
import com.dematic.labs.analytics.common.cassandra.Connections
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQueryListener


class CassandraStreamingQueryListener(val appName: String, val keySpace: String, val sparkConf: SparkConf)
  extends StreamingQueryListener {
  private val TABLE_NAME = "ss_cassandra_query_statistics"

  private def createTableCql(keyspace: String): String = {
    String.format("CREATE TABLE if not exists %s.%s (" +
      " id text," +
      " time timestamp," +
      " json text," +
      " PRIMARY KEY ((id), time))" +
      " WITH CLUSTERING ORDER BY (time DESC);", keyspace, TABLE_NAME)
  }

  // create the cassandra connector
  private val cassandraConnector = CassandraConnector.apply(sparkConf)
  // create the cassandra table
  Connections.createTable(createTableCql(keySpace), cassandraConnector)

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    println(event.toString)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val insert = QueryBuilder.insertInto(keySpace, TABLE_NAME)
      .value("id", appName)
      .value("time", event.progress.timestamp)
      .value("json", event.progress.json)
    Connections.execute(insert, cassandraConnector)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println(event.toString)
  }
}
