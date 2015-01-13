package com.dematic.labs.analytics;

import org.joda.time.ReadableInstant;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * Event needs to be defined,
 */
public final class Event {
    // todo: temporary
    private final UUID jobId;
    private final String facilityId;
    private final String nodeId;
    private final ReadableInstant startJobTime;
    private final ReadableInstant endJobTime;

    public Event(@Nonnull final UUID jobId, @Nonnull final String facilityId, @Nonnull final String nodeId,
                 @Nonnull final ReadableInstant startJobTime, @Nonnull final ReadableInstant endJobTime) {
        this.jobId = jobId;
        this.facilityId = facilityId;
        this.nodeId = nodeId;
        this.startJobTime = startJobTime;
        this.endJobTime = endJobTime;
    }

    public UUID getJobId() {
        return jobId;
    }

    public String getFacilityId() {
        return facilityId;
    }

    public String getNodeId() {
        return nodeId;
    }

    //todo: implement serializers...

    public String getStartJobTime() {
        return startJobTime.toString();
    }

    public String getEndJobTime() {
        return endJobTime.toString();
    }
}
