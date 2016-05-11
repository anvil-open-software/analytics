package com.dematic.labs.analytics.common.spark;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface StreamConfig extends Serializable {
    String getStreamEndpoint();
    void setStreamEndpoint(final String streamEndpoint);

    String getStreamName();
    void setStreamName(final String streamName);

    Map<String, String> getAdditionalConfiguration();
    void setAdditionalConfiguration(final Map<String, String> additionalConfiguration);

    Set<String> getTopics();
}
