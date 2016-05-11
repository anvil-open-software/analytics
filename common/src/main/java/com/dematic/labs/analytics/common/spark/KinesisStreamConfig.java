package com.dematic.labs.analytics.common.spark;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class KinesisStreamConfig implements StreamConfig, Serializable {
    private String streamEndpoint;
    private String streamName;

    @Override
    public String getStreamEndpoint() {
        return streamEndpoint;
    }

    @Override
    public void setStreamEndpoint(final String streamEndpoint) {
        this.streamEndpoint = streamEndpoint;
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public void setStreamName(final String streamName) {
        this.streamName = streamName;
    }

    @Override
    public Map<String, String> getAdditionalConfiguration() {
        throw new IllegalArgumentException("Not supported with Kinesis Stream");
    }

    @Override
    public void setAdditionalConfiguration(Map<String, String> configuration) {
        throw new IllegalArgumentException("Not supported with Kinesis Stream");
    }

    @Override
    public Set<String> getTopics() {
        throw new IllegalArgumentException("Not supported with Kinesis Stream");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KinesisStreamConfig that = (KinesisStreamConfig) o;
        return Objects.equals(streamEndpoint, that.streamEndpoint) &&
                Objects.equals(streamName, that.streamName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamEndpoint, streamName);
    }

    @Override
    public String toString() {
        return "KinesisStreamConfig{" +
                "streamEndpoint='" + streamEndpoint + '\'' +
                ", streamName='" + streamName + '\'' +
                '}';
    }
}
