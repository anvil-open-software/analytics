package com.dematic.labs.analytics.ingestion.spark.drivers.grainger;

//{"OPCTagID":1549,"OPCMetricID":25,"Sum":7.875713721263E12,"Count":7316,"Average":1.0765054293689175E9,"Minimum":"1000954613","Maximum":"999917866"}

import com.dematic.labs.toolkit.communication.Signal;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

class SignalAggregation implements Serializable {
    private final String opcTagId;
    private Long count;
    private Long sum;
    private Long min;
    private Long max;

    public SignalAggregation(final String opcTagId) {
        this.opcTagId = opcTagId;
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

    public String getOpcTagId() {
        return opcTagId;
    }

    public Long getCount() {
        return count;
    }

    public Long getSum() {
        return sum;
    }

    public Long getMin() {
        return min;
    }

    public Long getMax() {
        return max;
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
                Objects.equals(count, that.count) &&
                Objects.equals(sum, that.sum) &&
                Objects.equals(min, that.min) &&
                Objects.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opcTagId, count, sum, min, max);
    }

    @Override
    public String toString() {
        return "SignalAggregation{" +
                "opcTagId='" + opcTagId + '\'' +
                ", count=" + count +
                ", sum=" + sum +
                ", min=" + min +
                ", max=" + max +
                ", avg=" + getAvg() +
                '}';
    }
}
