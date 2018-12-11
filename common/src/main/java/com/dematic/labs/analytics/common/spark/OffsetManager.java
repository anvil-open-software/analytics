/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.common.spark;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.dematic.labs.analytics.common.kafka.Connections;
import com.google.common.base.Strings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.dematic.labs.analytics.common.cassandra.Connections.createTable;
import static com.dematic.labs.analytics.common.cassandra.Connections.execute;

@SuppressWarnings("unused")
public final class OffsetManager implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetManager.class);

    private static final String TABLE_NAME = "offsets";

    private static String createTableCql(final String keyspace) {
        return String.format("CREATE TABLE if not exists %s.%s (" +
                " topic text," +
                " partition int," +
                " from_offset bigint," +
                " to_offset bigint," +
                " PRIMARY KEY ((topic), partition))" +
                " WITH CLUSTERING ORDER BY (partition ASC);", keyspace, TABLE_NAME);
    }

    // Hold a reference to the current offset ranges, so it can be used downstream
    private static final AtomicReference<OffsetRange[]> OFFSET_RANGES = new AtomicReference<>();

    private OffsetManager() {
    }

    public static void setBatchOffsets(final OffsetRange[] offsetRanges) {
        OFFSET_RANGES.set(offsetRanges);
    }

    public static OffsetRange[] getBatchOffsetRanges() {
        return OFFSET_RANGES.get();
    }

    static OffsetRange[] loadOffsetRanges(final String keyspace, final String topic,
                                          final CassandraConnector connector) {
        // create table if not exist
        createTable(createTableCql(keyspace), connector);

        // get from data store
        final Statement stmt = QueryBuilder
                .select()
                .all()
                .from(keyspace, TABLE_NAME)
                .where(eq("topic", topic))
                .orderBy(desc("partition")).setConsistencyLevel(ConsistencyLevel.ALL);
        final ResultSet rs = execute(stmt, connector);
        final List<OffsetRange> offsetRanges = new ArrayList<>();
        while (!rs.isExhausted()) {
            // todo: may be more efficient ways to do this,
            final Row one = rs.one();
            final Integer partition = one.get("partition", Integer.class);
            final Long fromOffset = one.get("from_offset", Long.class);
            final Long toOffset = one.get("to_offset", Long.class);
            offsetRanges.add(OffsetRange.create(topic, partition, fromOffset, toOffset));
        }
        return offsetRanges.toArray(new OffsetRange[offsetRanges.size()]);
    }

    public static void saveOffsetRanges(final String keyspace, final OffsetRange[] offsetRanges,
                                        final CassandraConnector connector) {
        // create table if not exist
        createTable(createTableCql(keyspace), connector);

        // save to data store
        final List<RegularStatement> stmtList = new ArrayList<>();

        for (final OffsetRange offsetRange : offsetRanges) {
            final Insert stmt = QueryBuilder.insertInto(keyspace, TABLE_NAME)
                    .value("topic", offsetRange.topic())
                    .value("partition", offsetRange.partition())
                    .value("from_offset", offsetRange.fromOffset())
                    .value("to_offset", offsetRange.untilOffset());
            stmtList.add(stmt);
        }
        // execute
        execute(batch(stmtList.toArray(new RegularStatement[stmtList.size()]))
                .setConsistencyLevel(ConsistencyLevel.ALL), connector);
    }

    public static boolean manageOffsets() {
        return !Strings.isNullOrEmpty(System.getProperty(KafkaStreamConfig.KAFKA_OFFSET_MANAGE_KEY));
    }

    static boolean logOffsets() {
        return !Strings.isNullOrEmpty(System.getProperty(KafkaStreamConfig.KAFKA_OFFSET_LOG_KEY));
    }

    static List<TopicPartition> initialOffsets(final String serverIpAddress, final String topic) {
        final List<TopicPartition> topicPartitions = new ArrayList<>();
        try (final KafkaProducer<String, byte[]> kafkaProducer = Connections.getKafkaProducer(serverIpAddress)) {
            final List<PartitionInfo> partitionInfoList = kafkaProducer.partitionsFor(topic);
            partitionInfoList.forEach(partitionInfo -> {
                topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
                LOGGER.info("OFF: initial partition {}", partitionInfo);
            });
        }
        return topicPartitions;
    }
}
