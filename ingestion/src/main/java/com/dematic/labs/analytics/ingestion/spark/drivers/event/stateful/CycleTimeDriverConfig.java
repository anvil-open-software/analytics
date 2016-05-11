package com.dematic.labs.analytics.ingestion.spark.drivers.event.stateful;

import com.dematic.labs.analytics.common.spark.DynamoDbDriverConfig;

import java.util.Objects;

final class CycleTimeDriverConfig extends DynamoDbDriverConfig {
    private String bucketIncrementer;
    private String bucketSize;

    String getBucketIncrementer() {
        return bucketIncrementer;
    }

    void setBucketIncrementer(final String bucketIncrementer) {
        this.bucketIncrementer = bucketIncrementer;
    }

    String getBucketSize() {
        return bucketSize;
    }

    void setBucketSize(final String bucketSize) {
        this.bucketSize = bucketSize;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CycleTimeDriverConfig that = (CycleTimeDriverConfig) o;
        return Objects.equals(bucketIncrementer, that.bucketIncrementer) &&
                Objects.equals(bucketSize, that.bucketSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketIncrementer, bucketSize);
    }

    @Override
    public String toString() {
        return "CycleTimeDriverConfig{" +
                "bucketIncrementer='" + bucketIncrementer + '\'' +
                ", bucketSize='" + bucketSize + '\'' +
                "} " + super.toString();
    }
}
