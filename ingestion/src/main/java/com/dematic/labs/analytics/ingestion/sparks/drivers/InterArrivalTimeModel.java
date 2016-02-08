package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.toolkit.communication.Event;

import java.io.Serializable;
import java.util.List;

public final class InterArrivalTimeModel implements Serializable {
    private final String nodeId;
    private final List<Event> events;

    public InterArrivalTimeModel(final String nodeId, final List<Event> events) {
        this.nodeId = nodeId;
        this.events = events;
    }

    public String getNodeId() {
        return nodeId;
    }

    public List<Event> getEvents() {
        return events;
    }
}
