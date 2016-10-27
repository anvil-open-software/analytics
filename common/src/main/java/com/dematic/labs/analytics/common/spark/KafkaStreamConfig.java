package com.dematic.labs.analytics.common.spark;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class KafkaStreamConfig implements StreamConfig {
    public static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
    public static final String KAFKA_OFFSET_LOG_KEY = "com.dlabs.kafka.offset.debug.log";
    public static final String KAFKA_OFFSET_MANAGE_KEY = "com.dlabs.kafka.offset.manage";

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamConfig.class);

    private String streamEndpoint; // kafka bootstrap.servers
    private String streamName; // kafka topics
    private Map<String, String> additionalConfiguration;

    public KafkaStreamConfig() {
        additionalConfiguration = new HashMap<>();
        // any jvm property starting with kafka.additionalconfig.
        addPrefixedSystemProperties(additionalConfiguration, "kafka.additionalconfig.");
    }


    /**
     *
     * @param prefix
     * @return map with any system properties starting with prefix
     * todo could not find utility. but should be put in some generic utils class in toolkit
     */
    public static void addPrefixedSystemProperties(Map<String,String> properties, String prefix){
        for(String propName: System.getProperties().stringPropertyNames()){
            if(propName.startsWith(prefix)){
                String key = propName.substring(prefix.length());
                String value=  System.getProperty(propName);
                properties.put(key,value);
                LOGGER.info("Adding property for " + key + '=' + value);
            }
        }
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
