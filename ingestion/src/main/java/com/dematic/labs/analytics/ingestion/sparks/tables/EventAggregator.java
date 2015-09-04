package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

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

    private long count;

    public EventAggregator() {
    }

    public EventAggregator(final String bucket, final String eventType, final String created, final String updated,
                           final long count) {
        this.bucket = bucket;
        this.eventType = eventType;
        this.created = created;
        this.updated = updated;
        this.count = count;
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
    public long getCount() {
        return count;
    }

    public void setCount(final long count) {
        this.count = count;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final EventAggregator that = (EventAggregator) o;
        return Objects.equals(count, that.count) &&
                Objects.equals(bucket, that.bucket) &&
                Objects.equals(eventType, that.eventType) &&
                Objects.equals(created, that.created) &&
                Objects.equals(updated, that.updated);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, eventType, created, updated, count);
    }

    @Override
    public String toString() {
        return "EventAggregator{" +
                "bucket='" + bucket + '\'' +
                ", eventType='" + eventType + '\'' +
                ", created='" + created + '\'' +
                ", updated='" + updated + '\'' +
                ", count=" + count +
                '}';
    }
}
