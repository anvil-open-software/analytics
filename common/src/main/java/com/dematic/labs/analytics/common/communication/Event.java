/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.common.communication;

import org.joda.time.DateTime;
import org.joda.time.ReadableInstant;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Event needs to be defined,
 */
@SuppressWarnings("UnusedDeclaration")
public final class Event implements Serializable {
    public static final String TABLE_NAME = "Events";

    public static String createTableCql(final String keyspace) {
        return String.format("CREATE TABLE if not exists %s.\"%s\" (" +
                "\"id\" uuid PRIMARY KEY, " +
                "sequence bigint, " +
                "\"nodeId\" varchar, " +
                "\"jobId\" uuid, " +
                "type text, " +
                "timestamp varchar, " +
                "\"generatorId\" varchar" +
                ");", keyspace, TABLE_NAME);
    }

    private UUID id;
    private Long sequence;
    private String nodeId; // Node-135
    private UUID jobId; // correlation Id
    private EventType type;
    private ReadableInstant timestamp; // time events are generated
    private String generatorId;
    private Long version;

    public Event() {
        sequence = EventSequenceNumber.next();
    }

    Event(final UUID id, final Long sequence, final String nodeId, final UUID jobId, final EventType type,
          final ReadableInstant timestamp, final String generatorId, final Long version) {
        this.id = id;
        this.sequence = sequence;
        this.nodeId = nodeId;
        this.jobId = jobId;
        this.type = type;
        this.timestamp = timestamp;
        this.generatorId = generatorId;
        this.version = version;
    }

    public UUID getId() {
        return id;
    }

    public void setId(final UUID id) {
        this.id = id;
    }

    Long getSequence() {
        return sequence;
    }

    public void setSequence(final Long sequence) {
        this.sequence = sequence;
    }

    String getNodeId() {
        return nodeId;
    }

    public void setNodeId(final String nodeId) {
        this.nodeId = nodeId;
    }

    UUID getJobId() {
        return jobId;
    }

    public void setJobId(final UUID jobId) {
        this.jobId = jobId;
    }

    EventType getType() {
        return type;
    }

    public void setType(final EventType type) {
        this.type = type;
    }

    public ReadableInstant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final ReadableInstant timestamp) {
        this.timestamp = timestamp;
    }

    String getGeneratorId() {
        return generatorId;
    }

    public void setGeneratorId(final String generatorId) {
        this.generatorId = generatorId;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(final Long version) {
        this.version = version;
    }

    public String aggregateBy(final TimeUnit unit) {
        final String aggregateTime;
        switch (unit) {
            case MINUTES: {
                aggregateTime = new DateTime(getTimestamp()).minuteOfHour().roundFloorCopy().toDateTimeISO().toString();
                break;
            }
            case HOURS: {
                aggregateTime = new DateTime(getTimestamp()).hourOfDay().roundFloorCopy().toDateTimeISO().toString();
                break;
            }
            default: {
                throw new IllegalArgumentException(String.format(">%s< needs to be either {MINUTES, HOURS}",
                        unit));
            }
        }
        return aggregateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(sequence, event.sequence) &&
                Objects.equals(id, event.id) &&
                Objects.equals(nodeId, event.nodeId) &&
                Objects.equals(jobId, event.jobId) &&
                Objects.equals(type, event.type) &&
                Objects.equals(timestamp, event.timestamp) &&
                Objects.equals(generatorId, event.generatorId) &&
                Objects.equals(version, event.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, sequence, nodeId, jobId, type, timestamp, generatorId, version);
    }

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                ", sequence=" + sequence +
                ", nodeId='" + nodeId + '\'' +
                ", jobId='" + jobId + '\'' +
                ", type='" + type + '\'' +
                ", timestamp=" + timestamp +
                ", generatorId='" + generatorId + '\'' +
                ", version=" + version +
                '}';
    }
}
