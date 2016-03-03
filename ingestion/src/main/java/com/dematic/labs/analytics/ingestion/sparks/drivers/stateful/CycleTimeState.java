package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.collect.Multimap;

import java.io.Serializable;
import java.util.UUID;

public final class CycleTimeState implements Serializable {
    private final Multimap<UUID, Event> mapWithState;
    //private Long movingAverage;

    public CycleTimeState(final Multimap<UUID, Event> map) {
        this.mapWithState = map;
    }

    public void updateEvents(final Multimap<UUID, Event> newMap) {
        mapWithState.putAll(newMap);
    }

    public CycleTime createModel() {
        return new CycleTime();
    }
}
