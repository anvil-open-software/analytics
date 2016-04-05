package com.dematic.labs.analytics.store.sparks.drivers.cassandra;

import com.dematic.labs.analytics.common.spark.StreamFunctions;
import com.dematic.labs.toolkit.GenericBuilder;
import com.dematic.labs.toolkit.communication.Event;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.Charset;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;

public final class Persister implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Persister.class);
    private static final String RAW_EVENT_LEASE_TABLE_NAME = "Raw_Event_LT";

    // event stream processing function
    private static final class PersistFunction implements VoidFunction<JavaDStream<byte[]>> {
        private final PersisterDriverConfig driverConfig;

        PersistFunction(final PersisterDriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(final JavaDStream<byte[]> javaDStream) throws Exception {
            // transform the byte[] (byte arrays are json) to a string to events
            final JavaDStream<Event> eventStream =
                    javaDStream.map(event -> jsonToEvent(new String(event, Charset.defaultCharset())));
            eventStream.foreachRDD(rdd -> {
                javaFunctions(rdd).writerBuilder(driverConfig.getKeySpace(), Event.TABLE_NAME,
                        mapToRow(Event.class)).saveToCassandra();

            });
        }
    }

    public static void main(final String[] args) {
        // master url is only set for testing or running locally
        if (args.length < 3) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint, Kinesis StreamName, " +
                    "CassandraHost, KeySpace, optional driver MasterUrl, driver PollTime");
        }
        final String kinesisEndpoint = args[0];
        final String kinesisStreamName = args[1];
        final String masterUrl;
        final String host;
        final String keySpace;
        final String pollTime;
        if (args.length == 6) {
            masterUrl = args[2];
            host = args[3];
            keySpace = args[4];
            pollTime = args[5];
        } else {
            // no master url
            masterUrl = null;
            host = args[2];
            keySpace = args[3];
            pollTime = args[4];
        }
        // create the driver configuration and checkpoint dir
        final PersisterDriverConfig driverConfig = configure(RAW_EVENT_LEASE_TABLE_NAME, kinesisEndpoint,
                kinesisStreamName, host, keySpace, masterUrl, pollTime);
        driverConfig.setCheckPointDirectoryFromSystemProperties(true);
        //todo: create the cassandra table, if it does not exist

        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext = JavaStreamingContext.getOrCreate(driverConfig.getCheckPointDir(),
                new StreamFunctions.CreateStreamingContextFunction(driverConfig, new PersistFunction(driverConfig)));
        // set the cassandra configuration
        streamingContext.sc().getConf().set("spark.cassandra.connection.host", driverConfig.getHost());
        // authorization properties come from system properties
        streamingContext.sc().getConf().set("spark.cassandra.auth.username", driverConfig.getUsername());
        streamingContext.sc().getConf().set("spark.cassandra.auth.password", driverConfig.getPassword());

        // Start the streaming context and await termination
        LOGGER.info("CP: starting Cassendra Persister Driver with master URL >{}<",
                streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("CP: spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }

    private static PersisterDriverConfig configure(final String appName, final String kinesisEndpoint,
                                                   final String kinesisStreamName, final String host,
                                                   final String keySpace, final String masterUrl,
                                                   final String pollTime) {
        return GenericBuilder.of(PersisterDriverConfig::new)
                .with(PersisterDriverConfig::setAppName, appName)
                .with(PersisterDriverConfig::setKinesisEndpoint, kinesisEndpoint)
                .with(PersisterDriverConfig::setKinesisStreamName, kinesisStreamName)
                .with(PersisterDriverConfig::setHost, host)
                .with(PersisterDriverConfig::setKeySpace, keySpace)
                .with(PersisterDriverConfig::setMasterUrl, masterUrl)
                .with(PersisterDriverConfig::setPollTime, pollTime)
                .build();
    }
}
