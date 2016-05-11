package com.dematic.labs.analytics.common.spark;

import com.google.common.base.Strings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class KafkaStreamConfig implements StreamConfig {
    public static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";

    private String streamEndpoint; // kafka bootstrap.servers
    private String streamName; // kafka topics
    private Map<String, String> additionalConfiguration;

    public KafkaStreamConfig() {
        additionalConfiguration = new HashMap<>();
    }

    @Override
    public String getStreamEndpoint() {
        return streamEndpoint;
    }

    @Override
    public void setStreamEndpoint(final String streamEndpoint) {
        this.streamEndpoint = streamEndpoint;
        // need to add to the additional config for spark
        additionalConfiguration.put(BOOTSTRAP_SERVERS_KEY, streamEndpoint);
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
        return additionalConfiguration;
    }

    @Override
    public void setAdditionalConfiguration(final Map<String, String> additionalConfiguration) {
        this.additionalConfiguration = additionalConfiguration;
    }

    @Override
    public Set<String> getTopics() {
        if(Strings.isNullOrEmpty(streamName)) {
            throw new IllegalArgumentException("No kafka topics defined");
        }
        return new HashSet<>(Arrays.asList(streamName.split(",")));
    }
}
