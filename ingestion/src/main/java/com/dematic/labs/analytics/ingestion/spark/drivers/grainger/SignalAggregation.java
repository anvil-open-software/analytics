package com.dematic.labs.analytics.ingestion.spark.drivers.grainger;

//{"OPCTagID":1549,"OPCMetricID":25,"Sum":7.875713721263E12,"Count":7316,"Average":1.0765054293689175E9,"Minimum":"1000954613","Maximum":"999917866"}

import com.dematic.labs.toolkit.communication.Signal;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class SignalAggregation implements Serializable {
    private Long count;
    private Long sum;
    private Long min;
    private Long max;

    public SignalAggregation() {
        count = 0L;
        sum = 0L;
        min = 0L;
        max = 0L;
    }

    SignalAggregation computeAggregations(final List<Signal> values) {
        values.stream().forEach(signal -> computeAggregations(signal.getValue()));
        return this;
    }

    private SignalAggregation computeAggregations(final Long value) {
        incrementCount();
        minMax(value);
        sum(value);
        return this;
    }

    private void incrementCount() {
        count++;
    }

    private void minMax(final Long value) {
        min = min <= value ? min : value;
        max = max >= value ? max : value;
    }

    private void sum(final Long value) {
        sum = sum + value;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    public Long getMin() {
        return min;
    }

    public void setMin(Long min) {
        this.min = min;
    }

    public Long getMax() {
        return max;
    }

    public void setMax(Long max) {
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
        return Objects.equals(count, that.count) &&
                Objects.equals(sum, that.sum) &&
                Objects.equals(min, that.min) &&
                Objects.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, sum, min, max);
    }

    @Override
    public String toString() {
        return "SignalAggregation{" +
                "count=" + count +
                ", sum=" + sum +
                ", min=" + min +
                ", max=" + max +
                ", avg=" + getAvg() +
                '}';
    }
}
