package com.dematic.labs.analytics.ingestion.spark.drivers.event.stateful;

import com.dematic.labs.analytics.common.spark.DynamoDbDriverConfig;

import java.util.Objects;

public final class InterArrivalTimeDriverConfig extends DynamoDbDriverConfig {
    private String mediumInterArrivalTime;
    private String bufferTime;

    public String getMediumInterArrivalTime() {
        return mediumInterArrivalTime;
    }

    public void setMediumInterArrivalTime(final String mediumInterArrivalTime) {
        this.mediumInterArrivalTime = mediumInterArrivalTime;
    }

    public String getBufferTime() {
        return bufferTime;
    }

    public void setBufferTime(final String bufferTime) {
        this.bufferTime = bufferTime;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        InterArrivalTimeDriverConfig that = (InterArrivalTimeDriverConfig) o;
        return Objects.equals(mediumInterArrivalTime, that.mediumInterArrivalTime) &&
                Objects.equals(bufferTime, that.bufferTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), mediumInterArrivalTime, bufferTime);
    }

    @Override
    public String toString() {
        return "InterArrivalTimeDriverConfig{" +
                "mediumInterArrivalTime='" + mediumInterArrivalTime + '\'' +
                ", bufferTime='" + bufferTime + '\'' +
                "} " + super.toString();
    }
}
