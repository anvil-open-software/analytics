package com.dematic.labs.analytics.common.spark;

import com.google.common.base.Strings;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

public final class StreamFunctions implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamFunctions.class);
    private StreamFunctions() {
    }

    // create kafka dstream function
    private static final class CreateKafkaDStream implements Function0<JavaDStream<byte[]>> {
        private final StreamConfig streamConfig;
        private final JavaStreamingContext streamingContext;

        CreateKafkaDStream(final StreamConfig streamConfig, final JavaStreamingContext streamingContext) {
            this.streamConfig = streamConfig;
            this.streamingContext = streamingContext;
        }

        @Override
        public JavaDStream<byte[]> call() throws Exception {
            final JavaPairInputDStream<String, byte[]> directStream =
                    KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class, StringDecoder.class,
                            DefaultDecoder.class, streamConfig.getAdditionalConfiguration(), streamConfig.getTopics());

            final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

            // get the offsets first
            directStream.transformToPair(
                    new Function<JavaPairRDD<String, byte[]>, JavaPairRDD<String, byte[]>>() {
                        @Override
                        public JavaPairRDD<String, byte[]> call(JavaPairRDD<String, byte[]> rdd) throws Exception {
                            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                            offsetRanges.set(offsets);
                            // see if this part is even being called
                            for (final OffsetRange o : offsetRanges.get()) {
                                LOGGER.warn("OFFSET: "+ o.topic() + ' ' + o.partition() + ' ' + o.fromOffset() + ' ' + o.untilOffset());
                            }
                            return rdd;
                        }
                    });

            JavaDStream<byte[]> jsonByteRdd =directStream.map((Function<Tuple2<String, byte[]>, byte[]>) Tuple2::_2);

            // log the offsets
            if (System.getProperty("com.dlabs.kafka.offset.debug.log") != null) {
                jsonByteRdd.foreachRDD((VoidFunction<JavaRDD<byte[]>>) signalJavaRDD -> {
                    for (final OffsetRange o : offsetRanges.get()) {
                        LOGGER.warn("OFFSET: " + o.topic() + ' ' + o.partition() + ' ' + o.fromOffset() + ' ' + o.untilOffset());
                    }
                });
            }

            return jsonByteRdd;
        }
    }

    public static final class CreateKafkaCassandraStreamingContext implements Function0<JavaStreamingContext> {
        private final CassandraDriverConfig driverConfig;
        private final VoidFunction<JavaDStream<byte[]>> streamProcessor;

        public CreateKafkaCassandraStreamingContext(final CassandraDriverConfig driverConfig,
                                                    final VoidFunction<JavaDStream<byte[]>> streamProcessor) {
            this.driverConfig = driverConfig;
            this.streamProcessor = streamProcessor;
        }

        @Override
        public JavaStreamingContext call() throws Exception {
            // create spark configure
            final SparkConf sparkConfiguration = new SparkConf().setAppName(driverConfig.getAppName());
            // if master url set, apply
            if (!Strings.isNullOrEmpty(driverConfig.getMasterUrl())) {
                sparkConfiguration.setMaster(driverConfig.getMasterUrl());
            }
            // set the authorization
            sparkConfiguration.set(CassandraDriverConfig.AUTH_USERNAME_PROP, driverConfig.getUsername());
            sparkConfiguration.set(CassandraDriverConfig.AUTH_PASSWORD_PROP, driverConfig.getPassword());
            // set the connection host
            sparkConfiguration.set(CassandraDriverConfig.CONNECTION_HOST_PROP, driverConfig.getHost());
            // set the connection keep alive
            sparkConfiguration.set(CassandraDriverConfig.KEEP_ALIVE_PROP, driverConfig.getKeepAlive());
            // create the streaming context
            final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConfiguration,
                    driverConfig.getPollTimeInSeconds());

            final JavaDStream<byte[]> dStream =
                    new CreateKafkaDStream(driverConfig.getStreamConfig(), streamingContext).call();
            // work on the streams
            streamProcessor.call(dStream);
            // set the checkpoint dir
            streamingContext.checkpoint(driverConfig.getCheckPointDir());
            // return the streaming context
            return streamingContext;
        }
    }
}
