package com.dematic.labs.analytics.ingestion.drivers;

import com.dematic.labs.analytics.ingestion.sparks.drivers.EventStreamStatistics;
import com.dematic.labs.toolkit.communication.Event;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.dematic.labs.toolkit.communication.EventUtils.generateEvents;

public final class EventStreamStatisticsTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamStatisticsTest.class);

    @Test
    public void calculateStatisticsWithStatCounter() {
        try (final JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkStats").setMaster("local[*]")
                .set("spark.driver.allowMultipleContexts", "true"))) {

            final int numberOfValues = 100000;
            // 1) create a test data
            final List<Double> testData =
                    IntStream.range(0, numberOfValues).
                            mapToDouble(d -> d).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
            final JavaDoubleRDD rdd = sc.parallelizeDoubles(testData);
            final StatCounter statCounter = rdd.stats();

            LOGGER.info("Count:    " + statCounter.count());
            LOGGER.info("Min:      " + statCounter.min());
            LOGGER.info("Max:      " + statCounter.max());
            LOGGER.info("Sum:      " + statCounter.sum());
            LOGGER.info("Mean:     " + statCounter.mean());
            LOGGER.info("Variance: " + statCounter.variance());
            LOGGER.info("Stdev:    " + statCounter.stdev());
            Assert.assertEquals(numberOfValues, statCounter.count());
        }
    }

    @Ignore
    public void calculateStreamingStatistics() {
        final String kinesisEndpoint = System.getProperty("kinesisEndpoint");
        final String kinesisInputStream = System.getProperty("kinesisInputStream");
        // 1) start sparks in the back ground
        final ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(() -> {
            // call the driver to consume events and store in dynamoDB
            final String[] driverProperties = {kinesisEndpoint, kinesisInputStream};
            EventStreamStatistics.main(driverProperties);
        });
        // 2) ensure driver deployed

        // 3) generate the events and push
       /* final List<Event> events = kinesisStreamRule.generateEvents(500, 4, 9);
        events.stream().forEach(System.out::println);
        kinesisStreamRule.pushEvents(events);*/

        // wait for an hour
        Awaitility.await().atMost(new Duration(1, TimeUnit.HOURS))
                .until(() -> false);
    }

    @Ignore
    public void calculateStreamingStatistics1() {
        // 3) generate the events and push
        final List<Event> events = generateEvents(1000, 4, 9);
        events.stream().forEach(System.out::println);
        //kinesisStreamRule.pushEventsToKinesis(events);
    }
}
