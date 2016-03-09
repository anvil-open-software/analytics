package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;


import com.dematic.labs.analytics.common.spark.DriverConfig;

public final class CycleTimeDriverConfig extends DriverConfig {
    private String bucketIncrementer;
    private String bucketSize;

    public String getBucketIncrementer() {
        return bucketIncrementer;
    }

    public void setBucketIncrementer(final String bucketIncrementer) {
        this.bucketIncrementer = bucketIncrementer;
    }

    public String getBucketSize() {
        return bucketSize;
    }

    public void setBucketSize(final String bucketSize) {
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
