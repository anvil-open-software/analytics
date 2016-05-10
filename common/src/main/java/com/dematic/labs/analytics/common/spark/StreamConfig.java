package com.dematic.labs.analytics.common.spark;

import java.util.Map;

public interface StreamConfig {
    String getStreamEndpoint();
    void setStreamEndpoint(final String streamEndpoint);

    String getStreamName();
    void setStreamName(final String streamName);

    Map<String, String> getStreamConfiguration();
    void setStreamConfiguration(final Map<String, String> configuration);
}
