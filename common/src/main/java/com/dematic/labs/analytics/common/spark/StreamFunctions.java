package com.dematic.labs.analytics.common.spark;

import com.google.common.base.Strings;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.Serializable;

public final class StreamFunctions implements Serializable {
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
            final JavaInputDStream<ConsumerRecord<String, byte[]>> directStream =
                    KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, byte[]>Subscribe(streamConfig.getTopics(),
                                    streamConfig.getAdditionalConfiguration()));
            // get the dstream
            return directStream.map((Function<ConsumerRecord<String, byte[]>, byte[]>) ConsumerRecord::value);
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
