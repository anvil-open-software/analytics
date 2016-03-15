package com.dematic.labs.analytics.ingestion.sparks.drivers.stateless;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.util.StringUtils;
import com.dematic.labs.analytics.common.spark.DriverConfig;
import com.dematic.labs.analytics.common.spark.DriverConsts;
import com.dematic.labs.toolkit.aws.Connections;
import com.dematic.labs.toolkit.communication.Event;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix;
import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;

public final class SimpleEventStreamAggregator implements EventStreamProcessor<byte[]> {
    private static final long serialVersionUID = 8408398636569114334L;

    // functions
    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

    public void processEvents(final DriverConfig session, final JavaDStream<byte[]> javaDStream) {

        // transform the byte[] (byte arrays are json) to a string to events, and ensure distinct within stream
        final JavaDStream<Event> eventStream =
                javaDStream.map(
                        event -> jsonToEvent(new String(event, Charset.defaultCharset()))
                ).transform((Function<JavaRDD<Event>, JavaRDD<Event>>) JavaRDD::distinct);
        final TimeUnit timeUnit = session.getTimeUnit();
        // map to pairs and aggregate by key
        final JavaPairDStream<String, Long> aggregates = eventStream.mapToPair(event -> {
            // Downsampling: where we reduce the event’s ISO 8601 timestamp down to timeUnit precision,
            // so for instance “2015-06-05T12:54:43.064528” becomes “2015-06-05T12:54:00.000000” for minute.
            // This downsampling gives us a fast way of bucketing or aggregating events via this downsampled key
            return Tuple2.apply(event.aggregateBy(timeUnit), 1L);
        }).reduceByKey(SUM_REDUCER);

        // just add a flag to be able to turn off bucket writes to see if this is causing restart hangs
        boolean skipDynamoDBwrite = System.getProperty(DriverConsts.SPARK_DRIVER_SKIP_DYNAMODB_WRITE)!= null;

        // save counts
        aggregates.foreachRDD(rdd -> {
            final AmazonDynamoDBClient amazonDynamoDBClient = Connections.getAmazonDynamoDBClient(session.getDynamoDBEndpoint());
            String tablePrefix = session.getDynamoPrefix();
            final DynamoDBMapper dynamoDBMapper = StringUtils.isNullOrEmpty(tablePrefix) ?
                    new DynamoDBMapper(amazonDynamoDBClient) :
                    new DynamoDBMapper(amazonDynamoDBClient, new DynamoDBMapperConfig(withTableNamePrefix(tablePrefix)));
            final List<Tuple2<String, Long>> collect = rdd.collect();
            for (final Tuple2<String, Long> bucket : collect) {
                if (!skipDynamoDBwrite) {
                    AggregationDriverUtils.createOrUpdateDynamoDBBucket(bucket, dynamoDBMapper);
                }
            }
            return null;
        });
    }
}