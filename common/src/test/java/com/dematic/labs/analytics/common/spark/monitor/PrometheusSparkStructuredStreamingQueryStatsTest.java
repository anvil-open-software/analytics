package com.dematic.labs.analytics.common.spark.monitor;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.util.StatCounter;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public final class PrometheusSparkStructuredStreamingQueryStatsTest {

   // private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusSparkStructuredStreamingQueryStatsTest.class);

    @Test
    public void testStreaming() {
        // just test out machinery
        PrometheusStreamingQueryListener queryListener=
                new PrometheusStreamingQueryListener("PROMETHEUS_STATS_TEST", "10.207.220.65:9091");
        queryListener.setAddSparkQueryStats(false);
        for (int i=0; i<5; i++) {
            queryListener.onQueryProgress(null);
        }

    }


}

