package com.dematic.labs.analytics.ingestion.spark.drivers.signal;

//{"OPCTagID":1549,"OPCMetricID":25,"Sum":7.875713721263E12,"Count":7316,"Average":1.0765054293689175E9,"Minimum":"1000954613","Maximum":"999917866"}

import com.dematic.labs.toolkit.communication.Signal;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * select * from signal_aggregate where opc_tag_id='123' and aggregate < '2016-05-24T00:00:00.000Z';
 * select * from signal_aggregate where opc_tag_id = 140 and aggregate >= '2016-05-25T19:30Z';
 */
@SuppressWarnings("unused")
public class SignalAggregation implements Serializable {

    public static final String TABLE_NAME = "signal_aggregate";

    public static String createTableCql(final String keyspace) {
        return String.format("CREATE TABLE if not exists %s.%s (" +
                " opc_tag_id bigint," +
                " aggregate timestamp," +
                " count bigint," +
                " sum bigint," +
                " min bigint," +
                " max bigint," +
                " PRIMARY KEY ((opc_tag_id), aggregate))" +
                " WITH CLUSTERING ORDER BY (aggregate DESC);", keyspace, TABLE_NAME);
    }

    private Long opcTagId;
    private Date aggregate;
    private Long count;
    private Long sum;
    private Long min;
    private Long max;

    public SignalAggregation(final Long opcTagId) {
        this.opcTagId = opcTagId;
        count = 0L;
        sum = 0L;
    }

    public SignalAggregation(final Long opcTagId, final Date aggregate) {
        this.opcTagId = opcTagId;
        this.aggregate = aggregate;
        count = 0L;
        sum = 0L;
    }

    void computeAggregations(final List<Signal> values) {
        values.stream().forEach(signal -> computeAggregations(signal.getValue()));
    }

    private void computeAggregations(final Long value) {
        incrementCount();
        minMax(value);
        sum(value);
    }

    private void incrementCount() {
        count++;
    }

    private void minMax(final Long value) {
        if (min == null || min > value) {
            min = value;
        }
        if (max == null || max < value) {
            max = value;
        }
    }

    private void sum(final Long value) {
        sum = sum + value;
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

    public Long getSum() {
        return sum;
    }

    public void setSum(final Long sum) {
        this.sum = sum;
    }

    public Long getMin() {
        return min;
    }

    public void setMin(final Long min) {
        this.min = min;
    }

    public Long getMax() {
        return max;
    }

    public void setMax(final Long max) {
        this.max = max;
    }

    public Double getAvg() {
        return getCount() > 0 ? (double) getSum() / getCount() : 0.0d;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SignalAggregation that = (SignalAggregation) o;
        return Objects.equals(opcTagId, that.opcTagId) &&
                Objects.equals(aggregate, that.aggregate) &&
                Objects.equals(count, that.count) &&
                Objects.equals(sum, that.sum) &&
                Objects.equals(min, that.min) &&
                Objects.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opcTagId, aggregate, count, sum, min, max);
    }

    @Override
    public String toString() {
        return "SignalAggregation{" +
                "opcTagId=" + opcTagId +
                ", aggregate=" + aggregate +
                ", count=" + count +
                ", sum=" + sum +
                ", min=" + min +
                ", max=" + max +
                '}';
    }
}
