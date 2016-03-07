package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.util.Objects;

@SuppressWarnings("unused")
@DynamoDBTable(tableName = CycleTime.TABLE_NAME)
public final class CycleTime {
    public static final String TABLE_NAME = "Cycle_Time";

    private String nodeId;
    private String cycleTimeBuckets;
    private Long jobCount;

    public CycleTime() {
    }

    public CycleTime(final String nodeId, final String cycleTimeBuckets, final Long jobCount) {
        this.nodeId = nodeId;
        this.cycleTimeBuckets = cycleTimeBuckets;
        this.jobCount = jobCount;
    }

    @DynamoDBHashKey()
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    @DynamoDBAttribute
    public String getCycleTimeBuckets() {
        return cycleTimeBuckets;
    }

    public void setCycleTimeBuckets(final String cycleTimeBuckets) {
        this.cycleTimeBuckets = cycleTimeBuckets;
    }

    @DynamoDBAttribute
    public Long getJobCount() {
        return jobCount;
    }

    public void setJobCount(Long jobCount) {
        this.jobCount = jobCount;
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
                Objects.equals(cycleTimeBuckets, cycleTime.cycleTimeBuckets) &&
                Objects.equals(jobCount, cycleTime.jobCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, cycleTimeBuckets, jobCount);
    }

    @Override
    public String toString() {
        return "CycleTime{" +
                "nodeId='" + nodeId + '\'' +
                ", cycleTimeBuckets='" + cycleTimeBuckets + '\'' +
                ", jobCount=" + jobCount +
                '}';
    }
}
