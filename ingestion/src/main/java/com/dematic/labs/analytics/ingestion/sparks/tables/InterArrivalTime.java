package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.google.common.collect.Iterables;

import java.io.Serializable;
import java.util.Set;

@SuppressWarnings("unused")
@DynamoDBTable(tableName = InterArrivalTime.TABLE_NAME)
public final class InterArrivalTime implements Serializable {
    public static final String TABLE_NAME = "Inter_Arrival_Time";

    private String nodeId;
    private Set<String> buckets;
    private String eventType;
    private Long lastEventTime;
    private Long errorCount;
    private Long version;

    public InterArrivalTime() {
    }

    public InterArrivalTime(final String nodeId) {
        this.nodeId = nodeId;
    }

    @DynamoDBHashKey()
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(final String nodeId) {
        this.nodeId = nodeId;
    }

    @DynamoDBAttribute
    public Set<String> getBuckets() {
        return buckets;
    }

    public void setBuckets(final Set<String> buckets) {
        this.buckets = buckets;
    }

    @DynamoDBAttribute
    public String getEventType() {
        return eventType;
    }

    public void setEventType(final String eventType) {
        this.eventType = eventType;
    }

    public Long getLastEventTime() {
        return lastEventTime;
    }

    public void setLastEventTime(final Long lastEventTime) {
        this.lastEventTime = lastEventTime;
    }

    @DynamoDBAttribute
    public Long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(final Long errorCount) {
        this.errorCount = errorCount;
    }

    @DynamoDBVersionAttribute
    public Long getVersion() {
        return version;
    }

    public void setVersion(final Long version) {
        this.version = version;
    }

    public static String bucketToJson(final Set<Integer> bucket, final Long count) {
        // todo: come up with a json format or something
        return String.format("{ bucket: [%s, %s] , count: %s }", Iterables.get(bucket, 1), Iterables.get(bucket, 2),
                count);
    }
}
