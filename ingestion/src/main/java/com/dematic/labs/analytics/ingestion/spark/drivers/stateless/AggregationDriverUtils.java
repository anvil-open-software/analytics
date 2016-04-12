package com.dematic.labs.analytics.ingestion.spark.drivers.stateless;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.dematic.labs.analytics.common.spark.DriverConfig;
import com.dematic.labs.analytics.common.spark.DriverUtils;
import com.dematic.labs.analytics.ingestion.spark.tables.EventAggregator;
import com.google.common.base.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import static com.dematic.labs.toolkit.communication.EventUtils.nowString;

/**
 * Shared
 */
final class AggregationDriverUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationDriverUtils.class);
    private static final int MAX_RETRY = 3;

    private AggregationDriverUtils() {
        /* util class */
    }

    /**
     * To checkpoint, need to create the stream inside the factory before calling checkpoint.
     */
    static JavaStreamingContext initializeCheckpointedSparkSession(final DriverConfig session,
                                                                   final String masterUrl,
                                                                   final EventStreamProcessor aggregator) {
        final String checkPointDir = session.getCheckPointDir();
        final JavaStreamingContextFactory factory = () -> {
            // Spark config
            final SparkConf configuration = new SparkConf().
                    // sets the lease manager table name
                            setAppName(session.getAppName());
            if (!Strings.isNullOrEmpty(masterUrl)) {
                configuration.setMaster(masterUrl);
            }

            final JavaStreamingContext streamingContext = new JavaStreamingContext(configuration, session.getPollTime());

            // we must now create kinesis streams before we checkpoint
            LOGGER.warn("Creating Kinesis DStreams for " + session.getKinesisStreamName());
            JavaDStream kinesisDStream = DriverUtils.getJavaDStream(session.getKinesisEndpoint(),
                    session.getKinesisStreamName(), streamingContext);

            // Start the streaming context and await termination
            LOGGER.info("starting Event Aggregator Driver with master URL >{}<", streamingContext.sparkContext().master());
            //noinspection unchecked
            aggregator.processEvents(session, kinesisDStream);

            // we checkpoint last because if we try to run it before processing the stream, we end up with more events than the first run.
            LOGGER.warn("Checkpointing to " + checkPointDir);
            streamingContext.checkpoint(checkPointDir);

            return streamingContext;
        };
        return Strings.isNullOrEmpty(checkPointDir) ? factory.create() :
                JavaStreamingContext.getOrCreate(checkPointDir, factory);
    }

    static void createOrUpdateDynamoDBBucket(final Tuple2<String, Long> bucket,
                                             final DynamoDBMapper dynamoDBMapper) {
        int count = 1;
        do {
            EventAggregator eventAggregator = null;
            try {

                final PaginatedQueryList<EventAggregator> query = dynamoDBMapper.query(EventAggregator.class,
                        new DynamoDBQueryExpression<EventAggregator>().withHashKeyValues(
                                new EventAggregator().withBucket(bucket._1())));
                if (query == null || query.isEmpty()) {
                    // create
                    eventAggregator = new EventAggregator(bucket._1(), null, nowString(), null, bucket._2(), null);
                    dynamoDBMapper.save(eventAggregator);
                    break;
                } else {
                    // update
                    // only 1 should exists
                    eventAggregator = query.get(0);
                    eventAggregator.setUpdated(nowString());
                    eventAggregator.setCount(eventAggregator.getCount() + bucket._2());
                    dynamoDBMapper.save(eventAggregator);
                    break;
                }
            } catch (final Throwable any) {
                LOGGER.error("unable to save >{}< trying again {}", eventAggregator, count, any);
            } finally {
                count++;
            }
        } while (count <= MAX_RETRY);
    }
}
