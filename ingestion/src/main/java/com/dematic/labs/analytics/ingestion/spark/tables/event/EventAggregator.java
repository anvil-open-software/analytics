package com.dematic.labs.analytics.ingestion.spark.tables.event;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;

import java.io.Serializable;
import java.util.Objects;

@SuppressWarnings("unused")
@DynamoDBTable(tableName = EventAggregator.TABLE_NAME)
public final class EventAggregator implements Serializable {
    public static final String TABLE_NAME = "Event_Aggregator";

    private String bucket;
    private String eventType;

    private String created;
    private String updated;

    private Long count;
    private Long chunk;
    private Long version;

    public EventAggregator() {
    }

    public EventAggregator(final String bucket, final String eventType, final String created, final String updated,
                           final Long count, final Long chunk) {
        this.bucket = bucket;
        this.eventType = eventType;
        this.created = created;
        this.updated = updated;
        this.count = count;
        this.chunk = chunk;
    }

    @DynamoDBHashKey()
    public String getBucket() {
        return bucket;
    }

    public void setBucket(final String bucket) {
        this.bucket = bucket;
    }

    public EventAggregator withBucket(final String bucket) {
        setBucket(bucket);
        return this;
    }

    @DynamoDBAttribute(attributeName =  "eventType")
    public String getEventType() {
        return eventType;
    }

    public void setEventType(final String eventType) {
        this.eventType = eventType;
    }

    @DynamoDBAttribute
    public String getCreated() {
        return created;
    }

    public void setCreated(final String created) {
        this.created = created;
    }

    @DynamoDBAttribute
    public String getUpdated() {
        return updated;
    }

    public void setUpdated(final String updated) {
        this.updated = updated;
    }

    @DynamoDBAttribute
    public Long getCount() {
        return count;
    }

    public void setCount(final Long count) {
        this.count = count;
    }

    public Long getChunk() {
        return chunk;
    }

    @DynamoDBAttribute
    public void setChunk(Long chunk) {
        this.chunk = chunk;
    }

    @DynamoDBVersionAttribute
    public Long getVersion() { return version; }

    public void setVersion(final Long version) { this.version = version;}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventAggregator that = (EventAggregator) o;
        return Objects.equals(bucket, that.bucket) &&
                Objects.equals(eventType, that.eventType) &&
                Objects.equals(created, that.created) &&
                Objects.equals(updated, that.updated) &&
                Objects.equals(count, that.count) &&
                Objects.equals(chunk, that.chunk) &&
                Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, eventType, created, updated, count, chunk, version);
    }

    @Override
    public String toString() {
        return "EventAggregator{" +
                "bucket='" + bucket + '\'' +
                ", eventType='" + eventType + '\'' +
                ", created='" + created + '\'' +
                ", updated='" + updated + '\'' +
                ", count=" + count +
                ", chunk=" + chunk +
                ", version=" + version +
                '}';
    }
}