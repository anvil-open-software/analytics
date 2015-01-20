package com.dematic.labs.analytics.kinesis.consumer;

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.model.Record;
import com.dematic.labs.analytics.Event;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import javax.annotation.Nonnull;
import java.io.IOException;

public final class EventToByteArrayTransformer implements ITransformer<Event, byte[]> {
    private final ObjectMapper objectMapper;

    public EventToByteArrayTransformer() {
        this.objectMapper = new ObjectMapper()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false /* force ISO8601 */)
                .setSerializationInclusion(JsonInclude.Include.ALWAYS);
    }

    @Override
    public Event toClass(@Nonnull final Record record) throws IOException {
        return objectMapper.readValue(record.getData().array(), Event.class);
    }

    @Override
    public byte[] fromClass(@Nonnull final Event record) throws IOException {
        return objectMapper.writer().withDefaultPrettyPrinter().writeValueAsBytes(record);
    }
}
