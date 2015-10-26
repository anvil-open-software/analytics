package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.util.StringUtils;
import com.dematic.labs.analytics.ingestion.sparks.tables.EventAggregator;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Strings;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getJavaDStream;
import static com.dematic.labs.analytics.common.sparks.DriverUtils.getStreamingContext;
import static com.dematic.labs.toolkit.aws.Connections.createDynamoTable;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;
import static com.dematic.labs.toolkit.communication.EventUtils.nowString;

public final class EventStreamCheckpointOutsideFactoryAggregator implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamCheckpointOutsideFactoryAggregator.class);
    private static final int MAX_RETRY = 3;

    public static final String EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME = EventAggregator.TABLE_NAME + "_Checkpoint_Outside_LT";

    // functions
    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

    public static void main(final String[] args) {
        if (args.length < 5) {
            throw new IllegalArgumentException("Driver requires Kinesis Endpoint, Kinesis StreamName, DynamoDB Endpoint,"
                    + "optional DynamoDB Prefix, driver PollTime, and aggregation by time {MINUTES,DAYS}");
        }
        // url and stream name to pull events
        final String kinesisEndpoint = args[0];
        final String streamName = args[1];
        final String dynamoDBEndpoint = args[2];

        final String dynamoPrefix;
        final Duration pollTime;
        final TimeUnit timeUnit;
        if (args.length == 5) {
            dynamoPrefix = null;
            pollTime = Durations.seconds(Integer.valueOf(args[3]));
            timeUnit = TimeUnit.valueOf(args[4]);
        } else {
            dynamoPrefix = args[3];
            pollTime = Durations.seconds(Integer.valueOf(args[4]));
            timeUnit = TimeUnit.valueOf(args[5]);
        }

        final String appName = Strings.isNullOrEmpty(dynamoPrefix) ? EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME :
                String.format("%s%s", dynamoPrefix, EVENT_STREAM_AGGREGATOR_LEASE_TABLE_NAME);


        // checkpoint dir comes from the jvm params
        final String checkPointDir = System.getProperty("spark.checkpoint.dir");
        if (Strings.isNullOrEmpty(checkPointDir)) {
            throw new IllegalArgumentException("'spark.checkpoint.dir' jvm parameter needs to be set");
        }
        LOGGER.info("using >{}< checkpoint dir", checkPointDir);


        // create the table, if it does not exist
        createDynamoTable(dynamoDBEndpoint, EventAggregator.class, dynamoPrefix);
        //todo: master url will be set using the spark submit driver command
        // DO NOT PASS IN checkpoint here, do it after...
        final JavaStreamingContext streamingContext = getStreamingContext(null, appName, null, pollTime);

        // Start the streaming context and await termination
        LOGGER.info("starting Event Aggregator Driver with master URL >{}<", streamingContext.sparkContext().master());
        final EventStreamAggregator eventStreamAggregator = new EventStreamAggregator();
        JavaDStream<byte[]> javaDStream = getJavaDStream(kinesisEndpoint, streamName, streamingContext);

        // checkpoint after stream created
        streamingContext.checkpoint(checkPointDir);

        eventStreamAggregator.aggregateEvents(javaDStream, dynamoDBEndpoint, dynamoPrefix, timeUnit);


        streamingContext.start();
        LOGGER.info("spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }


}