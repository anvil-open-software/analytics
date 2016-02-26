package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@SuppressWarnings("unused")
@DynamoDBTable(tableName = CycleTime.TABLE_NAME)
public final class CycleTime {
    public static final String TABLE_NAME = "Cycle_Time";

    private String nodeId;

    @DynamoDBHashKey()
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
