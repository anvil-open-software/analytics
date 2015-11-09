package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.util.StringUtils;
import com.dematic.labs.analytics.common.sparks.DriverConfig;
import com.dematic.labs.analytics.ingestion.sparks.tables.EventAggregator;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getKinesisCheckpointWindow;
import static com.dematic.labs.analytics.ingestion.sparks.drivers.AggregationDriverUtils.createOrUpdateDynamoDBBucket;
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

    // functions
    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

    private static final class AggregateEventToBucketFunction implements PairFunction<Event, String, Long> {
        private final TimeUnit timeUnit;

        public AggregateEventToBucketFunction(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        @Override
        public Tuple2<String, Long> call(final Event event) throws Exception {
            // Downsampling: where we reduce the event’s ISO 8601 timestamp down to timeUnit precision,
            // so for instance “2015-06-05T12:54:43.064528” becomes “2015-06-05T12:54:00.000000” for minute.
            // This downsampling gives us a fast way of bucketing or aggregating events via this downsampled key
            return new Tuple2<>(event.aggregateBy(timeUnit), 1L);
        }
    }

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

    private static final class CreateDStreamFunction implements Function0<JavaDStream<byte[]>> {
        private final DriverConfig driverConfig;
        private final JavaStreamingContext streamingContext;

        public CreateDStreamFunction(final DriverConfig driverConfig, final JavaStreamingContext streamingContext) {
            this.driverConfig = driverConfig;
            this.streamingContext = streamingContext;
        }

        @Override
        public JavaDStream<byte[]> call() throws Exception {
            final String kinesisEndpoint = driverConfig.getKinesisEndpoint();
            final String streamName = driverConfig.getStreamName();
            // create the dstream
            final int shards = getNumberOfShards(kinesisEndpoint, streamName);
            // create 1 Kinesis Worker/Receiver/DStream for each shard
            final List<JavaDStream<byte[]>> streamsList = new ArrayList<>(shards);
            for (int i = 0; i < shards; i++) {
                streamsList.add(
                        KinesisUtils.createStream(streamingContext, streamName, kinesisEndpoint,
                                getKinesisCheckpointWindow(), InitialPositionInStream.TRIM_HORIZON,
                                StorageLevel.MEMORY_ONLY())
                );
            }
            // Union all the streams if there is more than 1 stream
            final JavaDStream<byte[]> unionStreams;
            if (streamsList.size() > 1) {
                unionStreams = streamingContext.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
            } else {
                unionStreams = streamsList.get(0);
            }
            return unionStreams;
        }
    }

    private static final class CreateStreamingContextFunction implements Function0<JavaStreamingContext> {
        private final DriverConfig driverConfig;

        public CreateStreamingContextFunction(final DriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public JavaStreamingContext call() throws Exception {
            // create spark configure
            final SparkConf sparkConfiguration = new SparkConf().setAppName(driverConfig.getAppName());
            // create the streaming context
            final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConfiguration,
                    driverConfig.getPollTime());
            // create the dstream
            final JavaDStream<byte[]> dStream =
                    new CreateDStreamFunction(driverConfig, streamingContext).call();
            // work on the streams
            new AggregateEventFunction(driverConfig).call(dStream);
            // set the checkpoint dir
            streamingContext.checkpoint(driverConfig.getCheckPointDir());
            // return the streaming context
            return streamingContext;
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
                JavaStreamingContext.getOrCreate(config.getCheckPointDir(), new CreateStreamingContextFunction(config));

        // Start the streaming context and await termination
        LOGGER.info("starting Event Aggregator Driver with master URL >{}<", streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }
}
