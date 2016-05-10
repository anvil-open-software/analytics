package com.dematic.labs.analytics.ingestion.spark.tables.event;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

@SuppressWarnings("unused")
@DynamoDBTable(tableName = CycleTime.TABLE_NAME)
public final class CycleTime implements Serializable {
    public static final String TABLE_NAME = "Cycle_Time";

    private String nodeId;
    private Set<String> buckets;
    private Long jobCount;
    private Long errorStartCount;
    private Long errorEndCount;

    public CycleTime() {
    }

    public CycleTime(final String nodeId, final Set<String> buckets, final Long jobCount, final Long errorStartCount,
                     final Long errorEndCount) {
        this.nodeId = nodeId;
        this.buckets = buckets;
        this.jobCount = jobCount;
        this.errorStartCount = errorStartCount;
        this.errorEndCount = errorEndCount;
    }

    @DynamoDBHashKey()
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
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
    public Long getJobCount() {
        return jobCount;
    }

    public void setJobCount(Long jobCount) {
        this.jobCount = jobCount;
    }

    @DynamoDBAttribute
    public Long getErrorStartCount() {
        return errorStartCount;
    }

    public void setErrorStartCount(Long errorStartCount) {
        this.errorStartCount = errorStartCount;
    }

    @DynamoDBAttribute
    public Long getErrorEndCount() {
        return errorEndCount;
    }

    public void setErrorEndCount(Long errorEndCount) {
        this.errorEndCount = errorEndCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CycleTime cycleTime = (CycleTime) o;
        return Objects.equals(nodeId, cycleTime.nodeId) &&
                Objects.equals(buckets, cycleTime.buckets) &&
                Objects.equals(jobCount, cycleTime.jobCount) &&
                Objects.equals(errorStartCount, cycleTime.errorStartCount) &&
                Objects.equals(errorEndCount, cycleTime.errorEndCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, buckets, jobCount, errorStartCount, errorEndCount);
    }

    @Override
    public String toString() {
        return "CycleTime{" +
                "nodeId='" + nodeId + '\'' +
                ", buckets=" + buckets +
                ", jobCount=" + jobCount +
                ", errorStartCount=" + errorStartCount +
                ", errorEndCount=" + errorEndCount +
                '}';
    }
}
