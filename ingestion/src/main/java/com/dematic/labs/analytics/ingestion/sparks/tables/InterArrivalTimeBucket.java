package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.io.Serializable;

@DynamoDBTable(tableName = InterArrivalTimeBucket.TABLE_NAME)
public final class InterArrivalTimeBucket implements Serializable {
    public static final String TABLE_NAME = "Inter_Arrival_Time_Bucket";
}
