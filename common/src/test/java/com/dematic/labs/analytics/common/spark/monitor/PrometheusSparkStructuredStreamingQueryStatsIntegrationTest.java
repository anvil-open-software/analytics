package com.dematic.labs.analytics.common.spark.monitor;

 // meant to be run manually for now since I don't have the monitor up all the time
public final class PrometheusSparkStructuredStreamingQueryStatsIntegrationTest {

   // private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusSparkStructuredStreamingQueryStatsTest.class);


    public void testStreaming() {
        // just test out machinery
        PrometheusStreamingQueryListener queryListener=
                new PrometheusStreamingQueryListener("PROMETHEUS_STATS_TEST", "10.207.220.65:9091");
        // need either to write a structured streaming stat

        queryListener.setAddSparkQueryStats(false);
        for (int i=0; i<5; i++) {
            queryListener.onQueryProgress(null);
        }

    }


}

