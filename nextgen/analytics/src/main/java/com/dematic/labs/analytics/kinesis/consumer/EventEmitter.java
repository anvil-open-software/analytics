package com.dematic.labs.analytics.kinesis.consumer;


import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * This class is used to store events to a data store. For now, we will just log events.
 */
public final class EventEmitter implements IEmitter<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventEmitter.class);

    public EventEmitter(@SuppressWarnings("UnusedParameters") @Nonnull final KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        // kinesisConnectorConfiguration contains data store information
    }

    @Override
    public List<byte[]> emit(@Nonnull final UnmodifiableBuffer<byte[]> buffer) throws IOException {
        // write to a data store
        final List<byte[]> eventRecords = buffer.getRecords();
        for (final byte[] eventRecord : eventRecords) {
            // just log for now
            LOGGER.info("KINESIS: Event : {}", new String(eventRecord));
        }
        return Collections.emptyList();
    }

    @Override
    public void fail(@Nonnull final List<byte[]> records) {
        for (final byte[] record : records) {
            LOGGER.error("KINESIS: failed to store event >{}<", new String(record));
        }
    }

    @Override
    public void shutdown() {
        LOGGER.info("KINESIS: shutting down consumer");
    }
}
