package com.dematic.labs.analytics.ingestion.drivers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public final class StatisticsTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsTest.class);

    @Test
    public void calculateStatistics() {
        // 1) create a test data
        final List<Double> testData =
                IntStream.range(1, 100000).
                        mapToDouble(d -> d).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        // 2) create a java spark context
        final JavaSparkContext sc =
                new JavaSparkContext(new SparkConf().setAppName("SparkStats").setMaster("local[*]"));
        final JavaDoubleRDD rdd = sc.parallelizeDoubles(testData);
        final StatCounter statCounter = rdd.stats();

        LOGGER.info("Count:    " + statCounter.count());
        LOGGER.info("Min:      " + statCounter.min());
        LOGGER.info("Max:      " + statCounter.max());
        LOGGER.info("Sum:      " + statCounter.sum());
        LOGGER.info("Mean:     " + statCounter.mean());
        LOGGER.info("Variance: " + statCounter.variance());
        LOGGER.info("Stdev:    " + statCounter.stdev());
    }
}
