package com.dematic.labs.analytics.common;

import org.joda.time.ReadableInstant;

import java.util.UUID;

/**
 * Event needs to be defined,
 */
@SuppressWarnings("UnusedDeclaration")
public final class Event {
    private UUID eventId;
    private int nodeId; // node 1 - 5
    private int jobId; // job 1 - 9
    private ReadableInstant timestamp; // time events are generated
    private double value; // random value

    public Event() {
    }

    public Event(final UUID eventId, final int nodeId, final int jobId, final ReadableInstant timestamp, final double value) {
        this.eventId = eventId;
        this.nodeId = nodeId;
        this.jobId = jobId;
        this.timestamp = timestamp;
        this.value = value;
    }

    public UUID getEventId() {
        return eventId;
    }

    public void setEventId(final UUID eventId) {
        this.eventId = eventId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(final int nodeId) {
        this.nodeId = nodeId;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(final int jobId) {
        this.jobId = jobId;
    }

    public ReadableInstant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final ReadableInstant timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(final double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Event{" +
                "eventId=" + eventId +
                ", nodeId=" + nodeId +
                ", jobId=" + jobId +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
