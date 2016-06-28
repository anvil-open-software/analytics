package com.dematic.labs.analytics.ingestion.spark.drivers.signal.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.dematic.labs.toolkit.SystemPropertyRule;
import com.dematic.labs.toolkit.cassandra.EmbeddedCassandraRule;
import com.dematic.labs.toolkit.communication.Signal;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.time.Instant;
import java.time.ZoneId;
import java.util.stream.IntStream;

public final class SignalAndSignalAggregation {

    private EmbeddedCassandraRule cassandraUnit = new EmbeddedCassandraRule();
    @Rule
    public final TestRule systemPropertyRule =
            RuleChain.outerRule(new SystemPropertyRule()).around(cassandraUnit);

    @Test
    public void querySignals() {
        // 1) ensure all keyspaces are clean
        cassandraUnit.dropKeySpace();
        // 2) create the keyspace name
        final String keyspace = System.getProperty("user.name") + "_keyspace";
        try (final Session session = cassandraUnit.getCluster().connect()) {
            // 3) create the keyspace
            session.execute("CREATE KEYSPACE " + keyspace + " WITH replication = {'class':'SimpleStrategy', " +
                    "'replication_factor':3};");
            // 4) create the table
            session.execute(Signal.createTableCql(keyspace));
            // 5) add signals
            insertSignals(session, keyspace, 50);
            // 6) query signals
            final ResultSet execute = session.execute("SELECT * FROM " + keyspace + "." + Signal.TABLE_NAME + ";");
            Assert.assertEquals(50, execute.all().size());
        }
    }

    private void insertSignals(final Session session, final String keyspace, final int numOfSignals) {
        final int[] ints = {1};
        IntStream.range(0, numOfSignals).forEach(signal -> {
            final ResultSet z = session.execute("INSERT INTO " + keyspace + "." + Signal.TABLE_NAME +
                    " (unique_id, id, value, day, timestamp, quality, opc_tag_reading_id, opc_tag_id, proxied_type_name, extended_properties) " +
                    "VALUES (" +
                    "'null'," +
                    ints[0] + "," +
                    ints[0] + "," +
                    "'2016-06-14T00:00:00Z'," +
                    "'" + Instant.now().atZone(ZoneId.of("Z")).toString() + "'," +
                    ints[0] + "," +
                    ints[0] + "," +
                    ints[0] + "," +
                    "'Odatech.Business.Integration.OPCTagReading'," +
                    "['test']);"
            );
            System.out.println(z.all());
            ints[0] = ints[0] + 1;
        });
    }

    @Test
    public void querySignalAggregation() {
        // ensure all keyspaces are clean
        cassandraUnit.dropKeySpace();
    }
}
