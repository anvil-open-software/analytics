package com.dematic.labs.analytics.ingestion.spark.drivers.stateful;


import com.dematic.labs.analytics.common.spark.DriverConfig;

final class CycleTimeDriverConfig extends DriverConfig {
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
    public String toString() {
        return "CycleTimeDriverConfig{" +
                "bucketIncrementer=" + bucketIncrementer +
                ", bucketSize=" + bucketSize +
                "} " + super.toString();
    }
}
