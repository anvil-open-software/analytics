package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.util.StringUtils;
import com.dematic.labs.analytics.common.sparks.DriverConfig;
import com.dematic.labs.analytics.ingestion.sparks.Functions.AggregateEventToBucketFunction;
import com.dematic.labs.analytics.ingestion.sparks.Functions.CreateStreamingContextFunction;
import com.dematic.labs.analytics.ingestion.sparks.tables.EventAggregator;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Strings;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.ingestion.sparks.drivers.AggregationDriverUtils.createOrUpdateDynamoDBBucket;
import static com.dematic.labs.analytics.ingestion.sparks.Functions.SUM_REDUCER;
import static com.dematic.labs.toolkit.aws.Connections.*;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;

public final class EventStreamCacheAggregator implements Serializable {
    public static final String EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME = EventAggregator.TABLE_NAME + "_Cache_LT";
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamCacheAggregator.class);

    // cache pool
    private static final JedisPool POOL;

    static {
        final String host = System.getProperty("dematic.cache.host");
        final String port = System.getProperty("dematic.cache.port");
        if (Strings.isNullOrEmpty(host) || Strings.isNullOrEmpty(port)) {
            throw new IllegalStateException(
                    String.format("'dematic.cache.host'=%s or 'dematic.cache.port'=%s is not set", host, port));
        }
        POOL = getCacheClientPool(host, Integer.parseInt(port));
    }

    // event stream processing function
    private static final class AggregateEventFunction implements VoidFunction<JavaDStream<byte[]>> {
        private final DriverConfig driverConfig;

        public AggregateEventFunction(final DriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(JavaDStream<byte[]> javaDStream) throws Exception {
            // transform the byte[] (byte arrays are json) to a string to events, and ensure distinct within stream and
            // filter against cache to ensure no duplicates
            final JavaDStream<Event> eventStream =
                    javaDStream.map(
                            event -> jsonToEvent(new String(event, Charset.defaultCharset()))
                    ).transform((Function<JavaRDD<Event>, JavaRDD<Event>>) JavaRDD::distinct)
                            .filter(event -> {
                                try (final Jedis cacheClient = POOL.getResource()) {
                                    final String uuid = event.getId().toString();
                                    return cacheClient.getSet(uuid, uuid) == null;
                                }
                            });

            // map to pairs and aggregate by key
            final JavaPairDStream<String, Long> aggregates = eventStream
                    .mapToPair(new AggregateEventToBucketFunction(driverConfig.getTimeUnit()))
                    .reduceByKey(SUM_REDUCER);

            // save counts
            aggregates.foreachRDD(rdd -> {
                final AmazonDynamoDBClient amazonDynamoDBClient =
                        getAmazonDynamoDBClient(driverConfig.getDynamoDBEndpoint());
                final DynamoDBMapper dynamoDBMapper = StringUtils.isNullOrEmpty(driverConfig.getDynamoPrefix()) ?
                        new DynamoDBMapper(amazonDynamoDBClient) :
                        new DynamoDBMapper(amazonDynamoDBClient,
                                new DynamoDBMapperConfig(withTableNamePrefix(driverConfig.getDynamoPrefix())));
                final List<Tuple2<String, Long>> collect = rdd.collect();
                for (final Tuple2<String, Long> bucket : collect) {
                    createOrUpdateDynamoDBBucket(bucket, dynamoDBMapper);
                }
                return null;
            });
        }
    }

    public static void main(final String[] args) {
        // set the configuration and checkpoint dir
        final DriverConfig config = new DriverConfig(EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME, args);
        config.setCheckPointDirectoryFromSystemProperties(true);
        // create the table, if it does not exist
        createDynamoTable(config.getDynamoDBEndpoint(), EventAggregator.class, config.getDynamoPrefix());
        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext =
                JavaStreamingContext.getOrCreate(config.getCheckPointDir(),
                        new CreateStreamingContextFunction(config, new AggregateEventFunction(config)));

        // Start the streaming context and await termination
        LOGGER.info("starting Event Aggregator Driver with master URL >{}<", streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }
}
