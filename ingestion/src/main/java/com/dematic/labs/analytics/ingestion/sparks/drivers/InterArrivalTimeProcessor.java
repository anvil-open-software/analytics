package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.analytics.common.sparks.DriverConfig;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTimeBucket;
import com.dematic.labs.toolkit.communication.Event;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dematic.labs.analytics.ingestion.sparks.Functions.CreateStreamingContextFunction;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;

public final class InterArrivalTimeProcessor implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(InterArrivalTimeProcessor.class);

    public static final String INTER_ARRIVAL_TIME_LEASE_TABLE_NAME = InterArrivalTimeBucket.TABLE_NAME + "_LT";

    // event stream processing function
    private static final class InterArrivalTimeFunction implements VoidFunction<JavaDStream<byte[]>> {
        private final DriverConfig driverConfig;

        public InterArrivalTimeFunction(final DriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(JavaDStream<byte[]> javaDStream) throws Exception {
            // transform the byte[] (byte arrays are json) to a string to events, and ensure distinct within stream and
            // and sort by timestamp
            final JavaDStream<Event> eventStream =
                    javaDStream.map(
                            event -> jsonToEvent(new String(event, Charset.defaultCharset())))
                            .transform((Function<JavaRDD<Event>, JavaRDD<Event>>) JavaRDD::distinct)
                            .transform(rdd -> rdd.sortBy(event -> event.getTimestamp().getMillis(), true,
                                    rdd.partitions().size()));
            // group by nodeId
            final JavaPairDStream<String, List<Event>> nodeToEventsPairs =
                    eventStream.mapToPair(event -> Tuple2.apply(event.getNodeId(), Collections.singletonList(event)));
            // todo: for now, we ar just going to print the inter arrival times
            final JavaPairDStream<String, List<Event>> nodeToEvents =
                    nodeToEventsPairs.reduceByKey((events1, events2) -> Stream.of(events1, events2)
                            .flatMap(Collection::stream).collect(Collectors.toList()));

            nodeToEvents.foreachRDD(rdd -> {
                //todo: should calculate and save to db, for now print
                rdd.collect().forEach(eventsByNode ->
                        LOGGER.info("node {} : event size {}", eventsByNode._1(), eventsByNode._2().size()));
                return null;
            });
        }
    }

    // functions
    public static void main(final String[] args) {
        // set the configuration and checkpoint dir
        final DriverConfig config = new DriverConfig(INTER_ARRIVAL_TIME_LEASE_TABLE_NAME);
        config.setParametersFromArgumentsForInterArrivalTime(args);
        config.setCheckPointDirectoryFromSystemProperties(true);
        // create the table, if it does not exist
        createDynamoTable(config.getDynamoDBEndpoint(), InterArrivalTimeBucket.class, config.getDynamoPrefix());
        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext =
                JavaStreamingContext.getOrCreate(config.getCheckPointDir(), new CreateStreamingContextFunction(config,
                        new InterArrivalTimeFunction(config)));

        // Start the streaming context and await termination
        LOGGER.info("starting Inter-ArrivalTime Driver with master URL >{}<", streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }
}
