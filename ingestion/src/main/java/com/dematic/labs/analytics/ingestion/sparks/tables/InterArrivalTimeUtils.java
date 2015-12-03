package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;

public final class InterArrivalTimeUtils {
    private final static ObjectMapper objectMapper;

    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(InterArrivalTimeBucket.class, new InterArrivalTimeBucketSerializer());
        module.addDeserializer(InterArrivalTimeBucket.class, new InterArrivalTimeBucketDeserializer());
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(module);
    }

    private InterArrivalTimeUtils() {
    }

    public static InterArrivalTimeBucket jsonToInterArrivalTimeBucket(final String json) throws IOException {
        return objectMapper.readValue(json, InterArrivalTimeBucket.class);
    }

    public static String interArrivalTimeBucketToJson(final InterArrivalTimeBucket bucket) throws IOException {
        return objectMapper.writeValueAsString(bucket);
    }

    private final static class InterArrivalTimeBucketSerializer extends JsonSerializer<InterArrivalTimeBucket> {
        @Override
        public void serialize(final InterArrivalTimeBucket bucket, final JsonGenerator jsonGenerator,
                              final SerializerProvider provider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("low", Integer.toString(bucket.getLowerBoundry()));
            jsonGenerator.writeStringField("high", Integer.toString(bucket.getUpperBoundry()));
            jsonGenerator.writeStringField("count", Long.toString(bucket.getCount()));
            jsonGenerator.writeEndObject();
        }
    }

    private final static class InterArrivalTimeBucketDeserializer extends JsonDeserializer<InterArrivalTimeBucket> {
        @Override
        public InterArrivalTimeBucket deserialize(final JsonParser jp,
                                                  final DeserializationContext deserializationContext)
                throws IOException {
            final ObjectCodec codec = jp.getCodec();
            final JsonNode jsonNode = codec.readTree(jp);
            final JsonNode eventLowNode = jsonNode.get("low");
            if (eventLowNode == null) {
                throw new IllegalStateException("InterArrivalTimeBucket does not have an low pair value");
            }
            final int low = eventLowNode.asInt();

            final JsonNode eventHighNode = jsonNode.get("high");
            if (eventHighNode == null) {
                throw new IllegalStateException("InterArrivalTimeBucket does not have an high pair value");
            }
            final int high = eventHighNode.asInt();

            final JsonNode eventCountNode = jsonNode.get("count");
            if (eventCountNode == null) {
                throw new IllegalStateException("InterArrivalTimeBucket does not have an count value");
            }
            final long count = eventCountNode.asLong();

            return new InterArrivalTimeBucket(low, high, count);
        }
    }
}
