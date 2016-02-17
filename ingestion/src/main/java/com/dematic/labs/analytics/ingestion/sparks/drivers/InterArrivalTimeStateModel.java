package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.toolkit.communication.Event;

import java.io.Serializable;
import java.util.List;

public final class InterArrivalTimeStateModel implements Serializable {
    private final String nodeId;
    private final Long lastEventTime;
    // buffered events, used when keeping state.
    private final List<Event> events;

    public InterArrivalTimeStateModel(final String nodeId, final Long lastEventTime, final List<Event> events) {
        this.nodeId = nodeId;
        this.lastEventTime = lastEventTime;
        this.events = events;
    }

    public String getNodeId() {
        return nodeId;
    }

    public Long getLastEventTime() {
        return lastEventTime;
    }

    public List<Event> getEvents() {
        return events;
    }
}
