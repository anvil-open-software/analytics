package com.dematic.labs.analytics.common.cassandra;


import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

public final class Connections {
    private Connections() {
    }

    public static void createTable(final String cql, final CassandraConnector connector) {
        try (Session session = connector.openSession()) {
            session.execute(cql);
        }
    }
}
