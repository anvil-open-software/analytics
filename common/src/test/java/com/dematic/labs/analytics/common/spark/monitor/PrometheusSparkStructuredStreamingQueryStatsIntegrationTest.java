package com.dematic.labs.analytics.common.spark.monitor;

import org.junit.Ignore;
import org.junit.Test;

/**
 *  Meant to be run manually for now since I don't have a wasy way of bringing up a push gateway on local
 *  and we should not put it into our actual monitor.
 *
 *
 */

public final class PrometheusSparkStructuredStreamingQueryStatsIntegrationTest {

   // @Ignore
    @Test
    public void pushTestStreamingStats() {
        // just test out machinery
        PrometheusStreamingQueryListener queryListener=
                new PrometheusStreamingQueryListener("PROMETHEUS_STATS_TEST", "10.207.220.65:9091", "test-cluster");
        // need either to write a structured streaming query to run or create mock data
        // was not trivial to create a mock results due to complex structures needed for progress stats

        queryListener.setAddSparkQueryStats(false);
        for (int i=0; i<6; i++) {
            queryListener.onQueryProgress(null);
        }

    }

}

