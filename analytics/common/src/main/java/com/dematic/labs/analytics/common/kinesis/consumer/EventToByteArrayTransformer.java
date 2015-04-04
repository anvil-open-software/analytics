package com.dematic.labs.analytics.common.kinesis.consumer;

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.model.Record;
import com.dematic.labs.analytics.common.Event;
import com.dematic.labs.analytics.common.EventUtils;

import java.io.IOException;
import java.nio.charset.Charset;

public final class EventToByteArrayTransformer implements ITransformer<Event, byte[]> {
    @Override
    public Event toClass(final Record record) throws IOException {
        return EventUtils.jsonToEvent(new String(record.getData().array(), Charset.defaultCharset()));
    }

    @Override
    public byte[] fromClass(final Event record) throws IOException {
        return EventUtils.eventToJsonByteArray(record);
    }
}
