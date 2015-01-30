package com.dematic.labs.producers;

import org.joda.time.DateTime;
import org.joda.time.ReadableInstant;

import java.util.UUID;

/**
 * Event needs to be defined,
 */
@SuppressWarnings("UnusedDeclaration")
public final class Event {
    // todo: temporary
    private  UUID jobId;
    private  String facilityId;
    private  String nodeId;
    private  ReadableInstant startJobTime;
    private  ReadableInstant endJobTime;

    public Event() {
    }

    public Event(final UUID jobId, final String facilityId, final String nodeId,
                 final ReadableInstant startJobTime, final ReadableInstant endJobTime) {
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

    public void setJobId(UUID jobId) {
        this.jobId = jobId;
    }

    public void setFacilityId(String facilityId) {
        this.facilityId = facilityId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public void setStartJobTime(String startJobTime) {
        this.startJobTime = new DateTime(startJobTime);
    }

    public void setEndJobTime(String endJobTime) {
        this.endJobTime = new DateTime(endJobTime);
    }

    @Override
    public String toString() {
        return "Event{" +
                "jobId=" + jobId +
                ", facilityId='" + facilityId + '\'' +
                ", nodeId='" + nodeId + '\'' +
                ", startJobTime=" + startJobTime +
                ", endJobTime=" + endJobTime +
                '}';
    }
}
