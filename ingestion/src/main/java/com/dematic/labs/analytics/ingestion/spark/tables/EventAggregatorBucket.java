package com.dematic.labs.analytics.ingestion.spark.tables;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

@DynamoDBTable(tableName = EventAggregatorBucket.TABLE_NAME)
public final class EventAggregatorBucket implements Serializable {
    public static final String TABLE_NAME = "Event_Aggregator_Bucket";

    private String bucketId;
    private byte[] uuids;

    @SuppressWarnings("unused") // needed by dynamo
    public EventAggregatorBucket() {
    }

    public EventAggregatorBucket(final String bucketId, final byte[] uuids) {
        this.bucketId = bucketId;
        this.uuids = uuids;
    }

    @DynamoDBHashKey()
    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(final String bucketId) {
        this.bucketId = bucketId;
    }

    @DynamoDBAttribute
    public byte[] getUuids() {
        return uuids;
    }

    public void setUuids(final byte[] uuids) {
        this.uuids = uuids;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventAggregatorBucket that = (EventAggregatorBucket) o;
        return Objects.equals(bucketId, that.bucketId) &&
                Arrays.equals(uuids, that.uuids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, uuids);
    }

    @Override
    public String toString() {
        return "EventAggregatorBucket{" +
                "bucketId='" + bucketId + '\'' +
                ", uuids=" + Arrays.toString(uuids) +
                '}';
    }
}
