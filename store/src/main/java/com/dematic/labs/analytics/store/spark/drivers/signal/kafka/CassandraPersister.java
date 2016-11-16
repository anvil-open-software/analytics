package com.dematic.labs.analytics.store.spark.drivers.signal.kafka;

import com.dematic.labs.analytics.common.spark.CassandraDriverConfig;
import com.dematic.labs.analytics.common.spark.KafkaStreamConfig;
import com.dematic.labs.analytics.common.spark.StreamConfig;
import com.dematic.labs.toolkit.helpers.bigdata.communication.Signal;
import com.dematic.labs.toolkit.helpers.bigdata.communication.SignalUtils;
import com.dematic.labs.toolkit.helpers.common.GenericBuilder;
import com.google.common.base.Strings;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*

8 DEBUG Resetting offset for partition regr_grainger-8 to earliest offset. (org.apache.kafka.clients.consumer.internals.Fetcher)
9 DEBUG Resetting offset for partition regr_grainger-9 to latest offset. (org.apache.kafka.clients.consumer.internals.Fetcher)

8 TRACE Sending ListOffsetRequest {replica_id=-1,topics=[{topic=regr_grainger,partitions=[{partition=8,timestamp=-2}]}]} to broker 10.102.20.12:9092 (id: 12 rack: null) (org.apache.kafka.clients.consumer.internals.Fetcher)
9 TRACE Sending ListOffsetRequest {replica_id=-1,topics=[{topic=regr_grainger,partitions=[{partition=9,timestamp=-1}]}]} to broker 10.102.20.13:9092 (id: 13 rack: null) (org.apache.kafka.clients.consumer.internals.Fetcher)

8 TRACE Received ListOffsetResponse {responses=[{topic=regr_grainger,partition_responses=[{partition=8,error_code=0,timestamp=-1,offset=0}]}]} from broker 10.102.20.12:9092 (id: 12 rack: null) (org.apache.kafka.clients.consumer.internals.Fetcher)
9 TRACE Received ListOffsetResponse {responses=[{topic=regr_grainger,partition_responses=[{partition=9,error_code=0,timestamp=-1,offset=66724}]}]} from broker 10.102.20.13:9092 (id: 13 rack: null) (org.apache.kafka.clients.consumer.internals.Fetcher)

8 DEBUG Fetched {timestamp=-1, offset=0} for partition regr_grainger-8 (org.apache.kafka.clients.consumer.internals.Fetcher)
9 DEBUG Fetched {timestamp=-1, offset=66724} for partition regr_grainger-9 (org.apache.kafka.clients.consumer.internals.Fetcher)


2016-11-15 22:55:00,024] TRACE Sending ListOffsetRequest {replica_id=-1,topics=[{topic=regr_grainger,partitions=[{partition=8,timestamp=-2}]}]} to broker 10.102.20.12:9092 (id: 12 rack: null) (org.apache.kafka.clients.consumer.internals.Fetcher)
2016-11-15 22:55:00,025] TRACE Received ListOffsetResponse {responses=[{topic=regr_grainger,partition_responses=[{partition=8,error_code=0,timestamp=-1,offset=0}]}]} from broker 10.102.20.12:9092 (id: 12 rack: null) (org.apache.kafka.clients.consumer.internals.Fetcher)
2016-11-15 22:55:00,025] DEBUG Fetched {timestamp=-1, offset=0} for partition regr_grainger-8 (org.apache.kafka.clients.consumer.internals.Fetcher)

2016-11-15 22:55:00,025] DEBUG Resetting offset for partition regr_grainger-9 to latest offset. (org.apache.kafka.clients.consumer.internals.Fetcher)
2016-11-15 22:55:00,025] TRACE Sending ListOffsetRequest {replica_id=-1,topics=[{topic=regr_grainger,partitions=[{partition=9,timestamp=-1}]}]} to broker 10.102.20.13:9092 (id: 13 rack: null) (org.apache.kafka.clients.consumer.internals.Fetcher)
2016-11-15 22:55:00,051] TRACE Received ListOffsetResponse {responses=[{topic=regr_grainger,partition_responses=[{partition=9,error_code=0,timestamp=-1,offset=66724}]}]} from broker 10.102.20.13:9092 (id: 13 rack: null) (org.apache.kafka.clients.consumer.internals.Fetcher)
2016-11-15 22:55:00,051] DEBUG Fetched {timestamp=-1, offset=66724} for partition regr_grainger-9 (org.apache.kafka.clients.consumer.internals.Fetcher)

*/


public final class CassandraPersister {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraPersister.class);
    public static final String APP_NAME = "CASSANDRA_PERSISTER";

    private CassandraPersister() {
    }

    // signal stream processing function
    private static final class PersistFunction implements VoidFunction<JavaInputDStream<ConsumerRecord<String, byte[]>>> {
        private final CassandraDriverConfig driverConfig;

        PersistFunction(final CassandraDriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(final JavaInputDStream<ConsumerRecord<String, byte[]>> javaDStream) throws Exception {

            // transform the byte[] (byte arrays are json) to signals
            final JavaDStream<Signal> eventStream =
                    javaDStream.map((Function<ConsumerRecord<String, byte[]>, byte[]>) ConsumerRecord::value).map(SignalUtils::jsonByteArrayToSignal);

            eventStream.foreachRDD(rdd -> {
                final List<Signal> collect = rdd.collect();
                LOGGER.info("BUG: " + collect.size());
            });
        }
    }

    public static void main(final String[] args) throws Exception {
        // master url is only set for testing or running locally
        if (args.length < 3) {
            throw new IllegalArgumentException("Driver requires Kafka Server Bootstrap, Kafka topics" +
                    "CassandraHost, KeySpace, optional driver MasterUrl, driver PollTime");
        }
        final String kafkaServerBootstrap = args[0];
        final String kafkaTopics = args[1];
        final String masterUrl;
        final String host;
        final String keySpace;
        final String pollTime;

        //noinspection Duplicates
        if (args.length == 6) {
            host = args[2];
            keySpace = args[3];
            masterUrl = args[4];
            pollTime = args[5];
        } else {
            // no master url
            masterUrl = null;
            host = args[2];
            keySpace = args[3];
            pollTime = args[4];
        }


        // create the driver configuration and checkpoint dir
        final CassandraDriverConfig driverConfig = configure(String.format("%s_%s", keySpace, APP_NAME),
                kafkaServerBootstrap, kafkaTopics, host, keySpace, masterUrl, pollTime);

        driverConfig.setCheckPointDirectoryFromSystemProperties(true);

        final SparkConf sparkConfiguration = new SparkConf().setAppName(driverConfig.getAppName());
        // if master url set, apply
        if (!Strings.isNullOrEmpty(driverConfig.getMasterUrl())) {
            sparkConfiguration.setMaster(driverConfig.getMasterUrl());
        }

        // create the streaming context
        final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConfiguration, driverConfig.getPollTimeInSeconds());
        // log everything
        streamingContext.ssc().sc().setLogLevel("ALL");

        // create the consumer strategy for managing offsets, of no offset is given for a TopicPartition,
        // the committed offset (if applicable) or kafka param auto.offset.reset will be used.
        final ConsumerStrategy<String, byte[]> cs =
                ConsumerStrategies.<String, byte[]>Subscribe(driverConfig.getStreamConfig().getTopics(),
                        driverConfig.getStreamConfig().getAdditionalConfiguration());

        // create the stream
        final JavaInputDStream<ConsumerRecord<String, byte[]>> directStream =
                KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), cs);


        /*directStream.foreachRDD(rdd -> {
            LOGGER.info("OFFSET: " + rdd.rdd().name() + " " + rdd.toString());
            logOffsets(((HasOffsetRanges) rdd.rdd()).offsetRanges());
        });
*/
        new PersistFunction(driverConfig).call(directStream);

        // Start the streaming context and await termination
        LOGGER.info("KCP: starting Kafka Cassandra Persister Driver with master URL >{}<",
                streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("KCP: spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }

    private static CassandraDriverConfig configure(final String appName, final String kafkaServerBootstrap,
                                                   final String kafkaTopics, final String host, final String keySpace,
                                                   final String masterUrl, final String pollTime) {
        final StreamConfig kafkaStreamConfig = GenericBuilder.of(KafkaStreamConfig::new)
                .with(KafkaStreamConfig::setStreamEndpoint, kafkaServerBootstrap)
                .with(KafkaStreamConfig::setStreamName, kafkaTopics)
                .with(KafkaStreamConfig::setGroupId, appName)
                .build();

        return GenericBuilder.of(CassandraDriverConfig::new)
                .with(CassandraDriverConfig::setAppName, appName)
                .with(CassandraDriverConfig::setHost, host)
                .with(CassandraDriverConfig::setKeySpace, keySpace)
                .with(CassandraDriverConfig::setKeepAlive, keepAlive(pollTime))
                .with(CassandraDriverConfig::setMasterUrl, masterUrl)
                .with(CassandraDriverConfig::setPollTime, pollTime)
                .with(CassandraDriverConfig::setStreamConfig, kafkaStreamConfig)
                .build();
    }

    private static String keepAlive(final String pollTime) {
        // keep alive is pollTime + 5
        return String.valueOf(Integer.valueOf(pollTime) + 5 * 1000);
    }
}
