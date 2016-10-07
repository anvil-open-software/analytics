package com.dematic.labs.analytics.common.spark;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.dematic.labs.analytics.common.cassandra.Connections;
import com.google.common.base.Strings;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;

public final class OffsetManager implements Serializable {
    public static final String TABLE_NAME = "offsets";

    public static String createTableCql(final String keyspace) {
        return String.format("CREATE TABLE if not exists %s.%s (" +
                " topic text," +
                " partition bigint," +
                " from_offset bigint," +
                " to_offset bigint," +
                " PRIMARY KEY (topic), partition))" +
                " WITH CLUSTERING ORDER BY (partition DESC);", keyspace, TABLE_NAME);
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

    public static OffsetRange[] loadOffsetRanges(final String topic, final CassandraConnector connector) {
        // get from datastore
        //final ResultSet execute = Connections.execute("", connector);
        return null;
    }

    public static void saveOffsetRanges(final OffsetRange[] offsetRanges, final CassandraConnector connector) {
        // save to datastore
        final List<RegularStatement> stmtList = new ArrayList<>();

        for (final OffsetRange offsetRange : offsetRanges) {
            final Insert stmt = QueryBuilder.insertInto("xxx")
                    .value("topic", offsetRange.topic())
                    .value("partition", offsetRange.partition())
                    .value("name", offsetRange.fromOffset())
                    .value("tags", offsetRange.untilOffset());
            stmtList.add(stmt);
        }
        // execute
        final ResultSet execute =
                Connections.execute(
                        batch(stmtList.toArray(new RegularStatement[stmtList.size()]))
                                .setConsistencyLevel(ConsistencyLevel.ALL), connector);
    }

    public static boolean manageOffsets() {
        return !Strings.isNullOrEmpty(System.getProperty(KafkaStreamConfig.KAFKA_OFFSET_MANAGE_KEY));
    }

    public static boolean logOffsets() {
        return !Strings.isNullOrEmpty(System.getProperty(KafkaStreamConfig.KAFKA_OFFSET_LOG_KEY));
    }
}
