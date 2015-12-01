package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.amazonaws.services.dynamodbv2.datamodeling.*;

import java.io.Serializable;
import java.util.Set;

@SuppressWarnings("unused")
@DynamoDBTable(tableName = InterArrivalTimeBucket.TABLE_NAME)
public final class InterArrivalTimeBucket implements Serializable {
    public static final String TABLE_NAME = "Inter_Arrival_Time_Bucket";

    private String nodeId;
    private Long bucketId;
    private Set<Integer> bucket;
    private String eventType;
    private String lastEventTime;
    private Long interArrivalTimeCount;
    private Long errorCount;
    private Long version;

    public InterArrivalTimeBucket() {
    }

    @DynamoDBHashKey()
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(final String nodeId) {
        this.nodeId = nodeId;
    }

    @DynamoDBRangeKey
    public Long getBucketId() {
        return bucketId;
    }

    public void setBucketId(Long bucketId) {
        this.bucketId = bucketId;
    }

    @DynamoDBAttribute
    public Set<Integer> getBucket() {
        return bucket;
    }

    public void setBucket(final Set<Integer> bucket) {
        this.bucket = bucket;
    }

    @DynamoDBAttribute
    public String getEventType() {
        return eventType;
    }

    public void setEventType(final String eventType) {
        this.eventType = eventType;
    }

    public String getLastEventTime() {
        return lastEventTime;
    }

    public void setLastEventTime(final String lastEventTime) {
        this.lastEventTime = lastEventTime;
    }

    @DynamoDBAttribute
    public Long getInterArrivalTimeCount() {
        return interArrivalTimeCount;
    }

    public void setInterArrivalTimeCount(final Long interArrivalTimeCount) {
        this.interArrivalTimeCount = interArrivalTimeCount;
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
}
