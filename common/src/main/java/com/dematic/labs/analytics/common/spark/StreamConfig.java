package com.dematic.labs.analytics.common.spark;

import java.util.Map;
import java.util.Set;

public interface StreamConfig {
    String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
    String TOPICS_KEY = "topics";

    String getStreamEndpoint();
    void setStreamEndpoint(final String streamEndpoint);

    String getStreamName();
    void setStreamName(final String streamName);

    Map<String, String> getAdditionalConfiguration();
    void setAdditionalConfiguration(final Map<String, String> configuration);

    Set<String> getTopics();
}
