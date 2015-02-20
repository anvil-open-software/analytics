package com.dematic.labs.analytics.common.kinesis.consumer;

import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class is used to store events to a data store. For now, we will just log events.
 */
public final class EventEmitter implements IEmitter<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventEmitter.class);
    // for now just keep consumed events for testing, this will go away once events are stored and processed
    private Collection<byte[]> consumedEvents = new ArrayList<>();


    @Override
    public List<byte[]> emit(final UnmodifiableBuffer<byte[]> buffer) throws IOException {
        // write to a data store
        final List<byte[]> eventRecords = buffer.getRecords();
        for (final byte[] eventRecord : eventRecords) {
            // just log for now and add to event consumed list
            LOGGER.info("KINESIS: Event : {}", new String(eventRecord));
            consumedEvents.add(eventRecord);
        }
        return Collections.emptyList();
    }

    @Override
    public void fail(final List<byte[]> records) {
        for (final byte[] record : records) {
            LOGGER.error("KINESIS: failed to store event >{}<", new String(record));
        }
    }

    @Override
    public void shutdown() {
        LOGGER.info("KINESIS: shutting down consumer");
    }

    // just for testing
    public int getConsumedEventCount() {
        return consumedEvents.size();
    }
}