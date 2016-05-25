package com.dematic.labs.analytics.ingestion.spark.drivers.signal;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

/**
 * CREATE TABLE signal_aggregate_by_time (
 * id text,
 * aggregate timestamp,
 * count counter,
 * sum counter,
 * PRIMARY KEY ((id), aggregate)
 * ) WITH CLUSTERING ORDER BY (aggregate DESC);
 * <p>
 * update signal_aggregate_by_time set count = count + 1, sum = sum + 5 where id='123' and aggregate='2016-05-23T03:52:00.000Z';
 * select * from signal_aggregate_by_time where id='123' and aggregate < '2016-05-24T00:00:00.000Z';
 */

public final class SignalAggregationByTime implements Serializable {
    public static final String TABLE_NAME = "signal_aggregate_by_time";

    public static String createTableCql(final String keyspace) {
        return String.format("CREATE TABLE %s.%s (" +
                " opc_tag_id bigint," +
                " aggregate timestamp," +
                " count counter," +
                " sum counter," +
                " PRIMARY KEY ((opc_tag_id), aggregate))" +
                " WITH CLUSTERING ORDER BY (aggregate DESC);", keyspace, TABLE_NAME);
    }

    private Long opcTagId;
    private Date aggregate;
    private Long count;
    private Long sum;

    public SignalAggregationByTime() {
    }

    public SignalAggregationByTime(final Long opcTagId, final Date aggregate, final Long count, final Long sum) {
        this.opcTagId = opcTagId;
        this.aggregate = aggregate;
        this.count = count;
        this.sum = sum;
    }

    public Long getOpcTagId() {
        return opcTagId;
    }

    public void setOpcTagId(final Long opcTagId) {
        this.opcTagId = opcTagId;
    }

    public Date getAggregate() {
        return aggregate;
    }

    public void setAggregate(final Date aggregate) {
        this.aggregate = aggregate;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(final Long count) {
        this.count = count;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(final Long sum) {
        this.sum = sum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SignalAggregationByTime that = (SignalAggregationByTime) o;
        return Objects.equals(count, that.count) &&
                Objects.equals(sum, that.sum) &&
                Objects.equals(opcTagId, that.opcTagId) &&
                Objects.equals(aggregate, that.aggregate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opcTagId, aggregate, count, sum);
    }

    @Override
    public String toString() {
        return "SignalAggregationByTime{" +
                "opcTagId='" + opcTagId + '\'' +
                ", aggregate=" + aggregate +
                ", count=" + count +
                ", sum=" + sum +
                '}';
    }
}