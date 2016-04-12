package com.dematic.labs.analytics.ingestion.spark.drivers.grainger;

import com.dematic.labs.analytics.common.spark.CassandraDriverConfig;
import com.dematic.labs.analytics.common.spark.StreamFunctions;
import com.dematic.labs.toolkit.GenericBuilder;
import com.dematic.labs.toolkit.communication.Event;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

import static com.dematic.labs.toolkit.communication.EventUtils.jsonToEvent;


public final class ComputeMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(ComputeMetrics.class);
    public static final String COMPUTE_METRICS_APP_NAME = "GRAINGER_LT";


    // event stream compute metrics function
    private static final class ComputeMetricsFunction implements VoidFunction<JavaDStream<byte[]>> {
        private final CassandraDriverConfig driverConfig;

        ComputeMetricsFunction(final CassandraDriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public void call(final JavaDStream<byte[]> javaDStream) throws Exception {
            // transform the byte[] (byte arrays are json) to a string todo reading....
            final JavaDStream<Event> eventStream =
                    javaDStream.map(event -> jsonToEvent(new String(event, Charset.defaultCharset())));

            eventStream.foreachRDD(rdd -> {

                /**
                 * // Convert RDD[String] to RDD[case class] to DataFrame
                 JavaRDD<JavaRow> rowRDD = rdd.map(new Function<String, JavaRow>() {
                 public JavaRow call(String word) {
                 JavaRow record = new JavaRow();
                 record.setWord(word);
                 return record;
                 }
                 });
                 DataFrame wordsDataFrame = sqlContext.createDataFrame(rowRDD, JavaRow.class);

                 // Register as table
                 wordsDataFrame.registerTempTable("words");

                 // Do word count on table using SQL and print it
                 DataFrame wordCountsDataFrame =
                 sqlContext.sql("select word, count(*) as total from words group by word");
                 wordCountsDataFrame.show();
                 */


            });

            // get the sql context and use it for data frames
            final SQLContext sqlContext = SQLContext.getOrCreate(javaDStream.context().sparkContext());

            // compute metrics
            // 5 minutes
            sqlContext.sql(MetricsSql.FIVE_MINUTES).registerTempTable("FiveMinutesReadings");
            final DataFrame fiveMinuteSum = sqlContext.sql(MetricsSql.FIVE_MINUTE_SUM);
            final DataFrameWriter json = fiveMinuteSum.write().format("json");
            // save to cassandra //todo:

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
        final CassandraDriverConfig driverConfig = configure(String.format("%s_%s",keySpace, COMPUTE_METRICS_APP_NAME), kinesisEndpoint,
                kinesisStreamName, host, keySpace, masterUrl, pollTime);

        driverConfig.setCheckPointDirectoryFromSystemProperties(true);
        // master url will be set using the spark submit driver command
        final JavaStreamingContext streamingContext = JavaStreamingContext.getOrCreate(driverConfig.getCheckPointDir(),
                new StreamFunctions.CreateCassandraStreamingContextFunction(driverConfig, new ComputeMetricsFunction(driverConfig)));

        //todo: figure out what tabke looks like.... create the cassandra table, if it does not exist
        /*createTable(createTableCql(driverConfig.getKeySpace()),
                CassandraConnector.apply(streamingContext.sc().getConf()));*/

        // Start the streaming context and await termination
        LOGGER.info("CM: starting Compute Metrics Driver with master URL >{}<",
                streamingContext.sparkContext().master());
        streamingContext.start();
        LOGGER.info("CM: spark state: {}", streamingContext.getState().name());
        streamingContext.awaitTermination();
    }

    private static CassandraDriverConfig configure(final String appName, final String kinesisEndpoint,
                                                   final String kinesisStreamName, final String host,
                                                   final String keySpace, final String masterUrl,
                                                   final String pollTime) {
        return GenericBuilder.of(CassandraDriverConfig::new)
                .with(CassandraDriverConfig::setAppName, appName)
                .with(CassandraDriverConfig::setKinesisEndpoint, kinesisEndpoint)
                .with(CassandraDriverConfig::setKinesisStreamName, kinesisStreamName)
                .with(CassandraDriverConfig::setHost, host)
                .with(CassandraDriverConfig::setKeySpace, keySpace)
                .with(CassandraDriverConfig::setMasterUrl, masterUrl)
                .with(CassandraDriverConfig::setPollTime, pollTime)
                .build();
    }
}
