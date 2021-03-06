/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.common.communication;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("WeakerAccess")
public final class SignalUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SignalUtils.class);
    private final static ObjectMapper objectMapper;

    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(Signal.class, new SignalSerializer());
        module.addDeserializer(Signal.class, new SignalDeserializer());
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(module);
    }

    private SignalUtils() {
    }

    public static Signal jsonToSignal(final String json) throws IOException {
        return objectMapper.readValue(json, Signal.class);
    }

    @SuppressWarnings("unused")
    public static Signal jsonByteArrayToSignal(final byte[] json) throws IOException {
        return objectMapper.readValue(json, Signal.class);
    }

    public static String signalToJson(final Signal signal) throws IOException {
        return objectMapper.writeValueAsString(signal);
    }

    public static Timestamp toTimestampFromInstance(final Instant instant) {
        if (instant == null) {
            return null;
        }
        return Timestamp.from(instant);
    }

    public static Instant toInstantFromTimestamp(final Timestamp timestamp) {
        return ZonedDateTime.ofInstant(timestamp.toInstant(), ZoneId.of("Z")).toInstant();
    }

    private final static class SignalSerializer extends JsonSerializer<Signal> {
        @Override
        public void serialize(final Signal signal, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializers)
                throws IOException {
            try {
                jsonGenerator.writeStartArray();
                jsonGenerator.writeStartObject();
                // write the ExtendedProperties array
                jsonGenerator.writeFieldName("ExtendedProperties");
                jsonGenerator.writeStartArray();
                signal.getExtendedProperties().forEach(property -> {
                    try {
                        jsonGenerator.writeString(property);
                    } catch (final IOException ioe) {
                        LOGGER.error("Unexpected error writing property {}", property, ioe);
                    }
                });
                jsonGenerator.writeEndArray();
                jsonGenerator.writeStringField("ProxiedTypeName", signal.getProxiedTypeName());
                jsonGenerator.writeNumberField("OPCTagID", signal.getOpcTagId());
                jsonGenerator.writeNumberField("OPCTagReadingID", signal.getOpcTagReadingId());
                jsonGenerator.writeNumberField("Quality", signal.getQuality());
                jsonGenerator.writeStringField("Timestamp", toInstantFromTimestamp(signal.getTimestamp()).
                        truncatedTo(ChronoUnit.NANOS).toString());
                jsonGenerator.writeNumberField("Value", signal.getValue());
                jsonGenerator.writeNumberField("ID", signal.getId());
                if (Strings.isNullOrEmpty(signal.getUniqueId())) {
                    jsonGenerator.writeNullField("UniqueID");
                } else {
                    jsonGenerator.writeStringField("UniqueID", signal.getUniqueId());
                }
            } finally {
                jsonGenerator.writeEndObject();
                jsonGenerator.writeEndArray();
                jsonGenerator.close();
            }
        }
    }

    private final static class SignalDeserializer extends JsonDeserializer<Signal> {
        @Override
        public Signal deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException {
            final ObjectCodec codec = jp.getCodec();
            final JsonNode jsonNode = codec.readTree(jp);

            final JsonNode extendedPropertiesNode = jsonNode.findValue("ExtendedProperties");
            final List<String> extendedProperties = new ArrayList<>();
            if (extendedPropertiesNode != null) {
                for (final JsonNode next : extendedPropertiesNode) {
                    final String property = next.isNull() ? null : next.textValue();
                    extendedProperties.add(property);
                }
            }

            final JsonNode proxiedTypeNameNode = jsonNode.findValue("ProxiedTypeName");
            final String proxiedTypeName = proxiedTypeNameNode == null ? null : proxiedTypeNameNode.textValue();

            final JsonNode opcTagIDNode = jsonNode.findValue("OPCTagID");
            final Long opcTagID = opcTagIDNode == null ? null : opcTagIDNode.asLong();

            final JsonNode opcTagReadingIDNode = jsonNode.findValue("OPCTagReadingID");
            final Long opcTagReadingID = opcTagReadingIDNode == null ? null : opcTagReadingIDNode.asLong();

            final JsonNode qualityNode = jsonNode.findValue("Quality");
            final Long quality = qualityNode == null ? null : qualityNode.asLong();

            final JsonNode timestampNode = jsonNode.findValue("Timestamp");
            final Instant timestamp = timestampNode == null ? null : Instant.parse(timestampNode.textValue()).
                    truncatedTo(ChronoUnit.NANOS);

            final JsonNode valueNode = jsonNode.findValue("Value");
            final Long value = valueNode == null ? null : valueNode.asLong();

            final JsonNode idNode = jsonNode.findValue("ID");
            final Long id = idNode == null ? null : idNode.asLong();

            final JsonNode uniqueIDNode = jsonNode.findValue("UniqueID");
            final String uniqueID = uniqueIDNode == null ? null : uniqueId(uniqueIDNode);

            return new Signal(uniqueID, id, value, toDayString(timestamp), toTimestampFromInstance(timestamp),
                    quality, opcTagReadingID, opcTagID, proxiedTypeName, extendedProperties);
        }
    }

    private static String uniqueId(final JsonNode uniqueIDNode) {
        return uniqueIDNode.isNull() ? null : uniqueIDNode.textValue();
    }

    private static String toDayString(final Instant instant) {
        if (instant == null) {
            return null;
        }
        return instant.truncatedTo(ChronoUnit.DAYS).toString();
    }
}
