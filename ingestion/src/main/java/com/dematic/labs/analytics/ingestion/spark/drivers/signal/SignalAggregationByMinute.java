package com.dematic.labs.analytics.ingestion.spark.drivers.signal;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

/**
 * CREATE TABLE signal_aggregate_by_minute (
 * opcTagId text,
 * aggregate timestamp,
 * count counter,
 * sum counter,
 * PRIMARY KEY ((opcTagId), aggregate)
 * ) WITH CLUSTERING ORDER BY (aggregate DESC);
 * <p>
 * update signal_aggregate_by_minute set count = count + 1, sum = sum + 5 where opcTagId='123' and aggregate='2016-05-23T03:52:00.000Z';
 * select * from signal_aggregate_by_minute where opcTagId='123' and aggregate < '2016-05-24T00:00:00.000Z';
 */

public final class SignalAggregationByMinute implements Serializable {
    public static final String TABLE_NAME = "signal_aggregate_by_minute";

    public static String createTableCql(final String keyspace) {
        return String.format("CREATE TABLE %s.%s (\n" +
                " opcTagId text,\n" +
                " aggregate timestamp,\n" +
                " count counter,\n" +
                " sum counter,\n" +
                " PRIMARY KEY ((opcTagId), aggregate)\n" +
                " ) WITH CLUSTERING ORDER BY (aggregate DESC);", keyspace, TABLE_NAME);
    }

    private String opcTagId;
    private Date aggregate;
    private long count;
    private long sum;

    public String getOpcTagId() {
        return opcTagId;
    }

    public void setOpcTagId(final String opcTagId) {
        this.opcTagId = opcTagId;
    }

    public Date getAggregate() {
        return aggregate;
    }

    public void setAggregate(final Date aggregate) {
        this.aggregate = aggregate;
    }

    public long getCount() {
        return count;
    }

    public void setCount(final long count) {
        this.count = count;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(final long sum) {
        this.sum = sum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SignalAggregationByMinute that = (SignalAggregationByMinute) o;
        return count == that.count &&
                sum == that.sum &&
                Objects.equals(opcTagId, that.opcTagId) &&
                Objects.equals(aggregate, that.aggregate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opcTagId, aggregate, count, sum);
    }

    @Override
    public String toString() {
        return "SignalAggregationByMinute{" +
                "opcTagId='" + opcTagId + '\'' +
                ", aggregate=" + aggregate +
                ", count=" + count +
                ", sum=" + sum +
                '}';
    }
}