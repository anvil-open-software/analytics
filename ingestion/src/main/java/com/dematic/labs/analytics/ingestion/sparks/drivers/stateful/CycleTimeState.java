package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.collect.Multimap;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

public final class CycleTimeState implements Serializable {
    private final String nodeId;
    private final Multimap<UUID, Event> mapWithState;
    private Long movingAverage;
    private Long processedEventCount;
    private Long numberOfJobsProcessed;
    private Long unprocessedEventCount;

    public CycleTimeState(final String nodeId, final Multimap<UUID, Event> map) {
        this.nodeId = nodeId;
        this.mapWithState = map;
        movingAverage = 0L;
        processedEventCount = 0L;
        numberOfJobsProcessed = 0L;
        unprocessedEventCount = 0L;
    }

    public void updateEvents(final Multimap<UUID, Event> newMap) {
        mapWithState.putAll(newMap);
    }

    public CycleTime createModel() {
        movingAverage = calculateMovingAvg(movingAverage, mapWithState);
        numberOfJobsProcessed = calculateNumberOfEvents(processedEventCount, mapWithState);
        numberOfJobsProcessed = calculateNumberOfJobs(numberOfJobsProcessed, mapWithState);
        // todo: errors figure out
        final Set<String> errors = calculateErrors(unprocessedEventCount, mapWithState);
        return new CycleTime(nodeId, movingAverage, numberOfJobsProcessed, numberOfJobsProcessed, errors);
    }

    private static long calculateMovingAvg(final long movingAverage, final Multimap<UUID, Event> map) {
        return 0;
    }

    private static long calculateNumberOfEvents(final long processedEventCount, final Multimap<UUID, Event> map) {
        return processedEventCount + map.values().size();
    }

    private static long calculateNumberOfJobs(final long numberOfJobsProcessed, final Multimap<UUID, Event> map) {
        return numberOfJobsProcessed + map.keys().size();
    }

    //todo: figure out
    private static Set<String> calculateErrors(final long unprocessedEventCount, final Multimap<UUID, Event> map) {
        return Collections.emptySet();
    }

}
