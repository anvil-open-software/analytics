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
import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;

public class BucketUtils implements Serializable {
    private final static ObjectMapper objectMapper;

    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(Bucket.class, new BucketSerializer());
        module.addDeserializer(Bucket.class, new BucketDeserializer());
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(module);
    }

    public static Set<Bucket> createBuckets(final int avgTime) {
        // see https://docs.google.com/document/d/1J9mSW8EbxTwbsGGeZ7b8TVkF5lm8-bnjy59KpHCVlBA/edit# for specs
        final Set<Bucket> buckets = Sets.newTreeSet(new Comparator<Bucket>() {
            @Override
            public int compare(final Bucket b1, final Bucket b2) {
                return Integer.compare(b1.getLowerBoundry(), b2.getLowerBoundry());
            }
        });

        for (int i = 0; i < avgTime * 2; i++) {
            final int low = i * avgTime / 5;
            final int high = (i + 1) * avgTime / 5;
            if (high > avgTime * 2) {
                // add the last bucket
                buckets.add(new Bucket(low, Integer.MAX_VALUE, 0L));
                break;
            }
            buckets.add(new Bucket(low, high, 0L));
        }
        return buckets;
    }

    public static Set<Bucket> createCycleTimeBuckets(final int bucketIncrement, final int numberOfBuckets) {
        // see https://docs.google.com/document/d/1J9mSW8EbxTwbsGGeZ7b8TVkF5lm8-bnjy59KpHCVlBA/edit# for specs
        // todo: did not follow specs, the numbers did not work out
        final Set<Bucket> buckets = Sets.newTreeSet(new Comparator<Bucket>() {
            @Override
            public int compare(final Bucket b1, final Bucket b2) {
                return Integer.compare(b1.getLowerBoundry(), b2.getLowerBoundry());
            }
        });
        for (int i = 0; i < numberOfBuckets; i++) {
            final int low = i * bucketIncrement;
            buckets.add(new Bucket(low, low + bucketIncrement, 0L));
        }
        // add the last bucket
        buckets.add(new Bucket(bucketIncrement * numberOfBuckets, Integer.MAX_VALUE, 0L));
        return buckets;
    }

    public static void addToBucket(final long time, final Set<Bucket> buckets) {
        final java.util.Optional<Bucket> first = buckets.stream()
                .filter(bucket -> bucket.isWithinBucket(time)).findFirst();
        if (!first.isPresent()) {
            throw new IllegalStateException(String.format("Unexpected Error: time >%s< not contained within " +
                    "buckets >%s<", time, buckets));
        }
        final Bucket bucket = first.get();
        bucket.incrementCount();
    }

    public static Bucket jsonToTimeBucket(final String json) throws IOException {
        return objectMapper.readValue(json, Bucket.class);
    }

    public static String timeBucketToJson(final Bucket bucket) throws IOException {
        return objectMapper.writeValueAsString(bucket);
    }

    public static Set<String> bucketsToJson(final Set<Bucket> buckets) {
        final Set<String> bucketsString = Sets.newLinkedHashSet();
        buckets.stream().forEach(bucket -> bucketsString.add(bucket.toJson()));
        return bucketsString;
    }

    public static long bucketTimeInSeconds(final long bucketTimeInMs) {
        return bucketTimeInMs / 1000;
    }

    private final static class BucketSerializer extends JsonSerializer<Bucket> {
        @Override
        public void serialize(final Bucket bucket, final JsonGenerator jsonGenerator, final SerializerProvider provider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("low", Integer.toString(bucket.getLowerBoundry()));
            jsonGenerator.writeStringField("high", Integer.toString(bucket.getUpperBoundry()));
            jsonGenerator.writeStringField("count", Long.toString(bucket.getCount()));
            jsonGenerator.writeEndObject();
        }
    }

    private final static class BucketDeserializer extends JsonDeserializer<Bucket> {
        @Override
        public Bucket deserialize(final JsonParser jp, final DeserializationContext deserializationContext)
                throws IOException {
            final ObjectCodec codec = jp.getCodec();
            final JsonNode jsonNode = codec.readTree(jp);
            final JsonNode eventLowNode = jsonNode.get("low");
            if (eventLowNode == null) {
                throw new IllegalStateException("Bucket does not have an low pair value");
            }
            final int low = eventLowNode.asInt();

            final JsonNode eventHighNode = jsonNode.get("high");
            if (eventHighNode == null) {
                throw new IllegalStateException("Bucket does not have an high pair value");
            }
            final int high = eventHighNode.asInt();

            final JsonNode eventCountNode = jsonNode.get("count");
            if (eventCountNode == null) {
                throw new IllegalStateException("Bucket does not have an count value");
            }
            final long count = eventCountNode.asLong();

            return new Bucket(low, high, count);
        }
    }
}
