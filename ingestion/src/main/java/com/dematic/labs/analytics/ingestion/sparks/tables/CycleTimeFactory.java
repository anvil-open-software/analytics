package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;

public final class CycleTimeFactory {
    private CycleTimeFactory() {
    }

    public static CycleTime findCycleTimeByNodeId(final String nodeId, final DynamoDBMapper dynamoDBMapper) {
        // lookup by nodeId
        final PaginatedQueryList<CycleTime> query = dynamoDBMapper.query(CycleTime.class,
                new DynamoDBQueryExpression<CycleTime>()
                        .withHashKeyValues(new CycleTime(nodeId, null, null)));
        if (query == null || query.isEmpty()) {
            return null;
        }
        // only 1 should exists
        return query.get(0);
    }

    public static long getJobCount(final String nodeId, final DynamoDBMapper dynamoDBMapper) {
        final CycleTime cycleTime = findCycleTimeByNodeId(nodeId, dynamoDBMapper);
        return cycleTime != null ? cycleTime.getJobCount() : 0;
    }
}
