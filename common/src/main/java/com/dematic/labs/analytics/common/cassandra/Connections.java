package com.dematic.labs.analytics.common.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.spark.connector.cql.CassandraConnector;

public final class Connections {
    private Connections() {
    }

    public static void createTable(final String createTableCql, final CassandraConnector connector) {
        try (final Session session = connector.openSession()) {
            session.execute(createTableCql);
        }
    }

    public static ResultSet execute(final Statement statement, final CassandraConnector connector) {
        try (final Session session = connector.openSession()) {
            return session.execute(statement);
        }
    }
}
