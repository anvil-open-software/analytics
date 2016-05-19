package com.dematic.labs.analytics.ingestion.spark.drivers.signal;

import com.dematic.labs.analytics.common.spark.CassandraDriverConfig;

public final class ComputeCumulativeMetricsDriverConfig extends CassandraDriverConfig {
    private String aggregationSizeInMinutes;

    public String getAggregationSizeInMinutes() {
        return aggregationSizeInMinutes;
    }

    public void setAggregationSizeInMinutes(final String aggregationSizeInMinutes) {
        this.aggregationSizeInMinutes = aggregationSizeInMinutes;
    }
}
