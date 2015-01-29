package com.dematic.labs.producers.kinesis.consumer;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase;
import com.dematic.labs.producers.Event;

import javax.annotation.Nonnull;

public final class EventPipeline implements IKinesisConnectorPipeline<Event, byte[]> {
    private final IEmitter<byte[]> emitter;

    public EventPipeline(@Nonnull final IEmitter<byte[]> emitter) {
        this.emitter = emitter;
    }

    @Override
    public IEmitter<byte[]> getEmitter(@Nonnull final KinesisConnectorConfiguration configuration) {
        return emitter;
    }

    @Override
    public IBuffer<Event> getBuffer(@Nonnull final KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<>(configuration);
    }

    @Override
    public ITransformerBase<Event, byte[]> getTransformer(@Nonnull final KinesisConnectorConfiguration configuration) {
        return new EventToByteArrayTransformer();
    }

    @Override
    public IFilter<Event> getFilter(@Nonnull final KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<>();
    }
}
