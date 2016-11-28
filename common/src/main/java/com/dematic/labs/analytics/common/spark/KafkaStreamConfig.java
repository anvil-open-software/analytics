package com.dematic.labs.analytics.common.spark;

import com.google.common.base.Strings;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class KafkaStreamConfig implements StreamConfig {
    public static final String KAFKA_OFFSET_LOG_KEY = "com.dlabs.kafka.offset.debug.log";
    public static final String KAFKA_OFFSET_MANAGE_KEY = "com.dlabs.kafka.offset.manage";

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamConfig.class);

    private String streamName; // kafka topics
    private Map<String, Object> additionalConfiguration;

    public KafkaStreamConfig() {
        additionalConfiguration = new HashMap<>();
        // add default properties deserializers
        additionalConfiguration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        additionalConfiguration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        additionalConfiguration.put("auto.offset.reset", "earliest");
        //todo: for now its set to off, need to investigate more,
        //todo: see http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
        additionalConfiguration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // any jvm property starting with kafka.additionalconfig.
        addPrefixedSystemProperties(additionalConfiguration, "kafka.additionalconfig.");
    }


    /**
     * @param prefix
     * @return map with any system properties starting with prefix
     * todo could not find utility. but should be put in some generic utils class in toolkit
     */
    private static void addPrefixedSystemProperties(final Map<String, Object> properties, final String prefix) {
        for (String propName : System.getProperties().stringPropertyNames()) {
            if (propName.startsWith(prefix)) {
                String key = propName.substring(prefix.length());
                String value = System.getProperty(propName);
                properties.put(key, value);
                LOGGER.info("Adding property for " + key + '=' + value);
            }
        }
    }

    @Override
    public String getStreamEndpoint() {
        return additionalConfiguration.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) ?
                additionalConfiguration.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).toString() : null;
    }

    @Override
    public void setStreamEndpoint(final String streamEndpoint) {
        if (!Strings.isNullOrEmpty(streamEndpoint)) {
            // need to set and pass map to spark
            additionalConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, streamEndpoint);
        }
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
    public String getGroupId() {
        return additionalConfiguration.containsValue(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) ?
                additionalConfiguration.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).toString() : null;
    }

    @Override
    public void setGroupId(final String groupId) {
        if (!Strings.isNullOrEmpty(groupId)) {
            //  kafka, a unique string that identifies the consumer group
            additionalConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
    }

    @Override
    public Map<String, Object> getAdditionalConfiguration() {
        return additionalConfiguration;
    }

    @Override
    public void setAdditionalConfiguration(final Map<String, Object> additionalConfiguration) {
        this.additionalConfiguration = additionalConfiguration;
    }

    @Override
    public Set<String> getTopics() {
        if (Strings.isNullOrEmpty(streamName)) {
            throw new IllegalArgumentException("No kafka topics defined");
        }
        return new HashSet<>(Arrays.asList(streamName.split(",")));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaStreamConfig that = (KafkaStreamConfig) o;
        return Objects.equals(streamName, that.streamName) &&
                Objects.equals(additionalConfiguration, that.additionalConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamName, additionalConfiguration);
    }

    @Override
    public String toString() {
        return "KafkaStreamConfig{" +
                "streamName='" + streamName + '\'' +
                ", additionalConfiguration=" + additionalConfiguration +
                '}';
    }
}
