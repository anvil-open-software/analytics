package com.dematic.labs.analytics.ingestion.spark.drivers.signal;

import com.dematic.labs.analytics.common.spark.CassandraDriverConfig;

import java.util.Objects;

public final class ComputeCumulativeMetricsDriverConfig extends CassandraDriverConfig {
    private Aggregation aggregateBy;

    public Aggregation getAggregateBy() {
        return aggregateBy;
    }

    public void setAggregateBy(final Aggregation aggregateBy) {
        this.aggregateBy = aggregateBy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ComputeCumulativeMetricsDriverConfig that = (ComputeCumulativeMetricsDriverConfig) o;
        return aggregateBy == that.aggregateBy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), aggregateBy);
    }

    @Override
    public String toString() {
        return "ComputeCumulativeMetricsDriverConfig{" +
                "aggregateBy=" + aggregateBy +
                "} " + super.toString();
    }
}
