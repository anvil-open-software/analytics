package com.dematic.labs.producers.kinesis.consumer;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.*;
import com.dematic.labs.producers.Event;

public final class EventPipeline implements IKinesisConnectorPipeline<Event, byte[]> {
    private final IEmitter<byte[]> emitter;

    public EventPipeline(final IEmitter<byte[]> emitter) {
        this.emitter = emitter;
    }

    @Override
    public IEmitter<byte[]> getEmitter(final KinesisConnectorConfiguration configuration) {
        return emitter;
    }

    @Override
    public IBuffer<Event> getBuffer(final KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<>(configuration);
    }

    @Override
    public ITransformerBase<Event, byte[]> getTransformer(final KinesisConnectorConfiguration configuration) {
        return new EventToByteArrayTransformer();
    }

    @Override
    public IFilter<Event> getFilter(final KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<>();
    }
}
