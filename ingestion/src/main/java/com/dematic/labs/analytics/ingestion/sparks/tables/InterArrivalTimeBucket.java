package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.amazonaws.services.dynamodbv2.datamodeling.*;

import java.io.Serializable;
import java.util.Set;

@SuppressWarnings("unused")
@DynamoDBTable(tableName = InterArrivalTimeBucket.TABLE_NAME)
public final class InterArrivalTimeBucket implements Serializable {
    public static final String TABLE_NAME = "Inter_Arrival_Time_Bucket";

    private Long bucketId;
    private Set<Integer> bucket;

    private Long interArrivalTimeCount;
    private Long version;

    @DynamoDBHashKey()
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
    public Long getInterArrivalTimeCount() {
        return interArrivalTimeCount;
    }

    public void setInterArrivalTimeCount(final Long interArrivalTimeCount) {
        this.interArrivalTimeCount = interArrivalTimeCount;
    }

    @DynamoDBVersionAttribute
    public Long getVersion() {
        return version;
    }

    public void setVersion(final Long version) {
        this.version = version;
    }
}
