package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.util.Set;

@SuppressWarnings("unused")
@DynamoDBTable(tableName = CycleTime.TABLE_NAME)
public final class CycleTime {
    public static final String TABLE_NAME = "Cycle_Time";

    private String nodeId;

    private Long movingAverage;
    private Long numberOfEvents;
    private Long numberOfJobs;
    private Set<String> errors;

    @DynamoDBHashKey()
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }


    @DynamoDBAttribute
    public Long getMovingAverage() {
        return movingAverage;
    }

    public void setMovingAverage(Long movingAverage) {
        this.movingAverage = movingAverage;
    }

    @DynamoDBAttribute
    public Long getNumberOfEvents() {
        return numberOfEvents;
    }

    public void setNumberOfEvents(Long numberOfEvents) {
        this.numberOfEvents = numberOfEvents;
    }

    @DynamoDBAttribute
    public Long getNumberOfJobs() {
        return numberOfJobs;
    }

    public void setNumberOfJobs(Long numberOfJobs) {
        this.numberOfJobs = numberOfJobs;
    }

    @DynamoDBAttribute
    public Set<String> getErrors() {
        return errors;
    }

    public void setErrors(Set<String> errors) {
        this.errors = errors;
    }
}
