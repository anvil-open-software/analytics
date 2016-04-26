package com.dematic.labs.analytics.ingestion.spark.drivers.stateful;

import com.dematic.labs.analytics.ingestion.spark.tables.InterArrivalTime;
import com.dematic.labs.toolkit.communication.Event;
import org.junit.Test;

import java.util.List;

import static com.dematic.labs.toolkit.communication.EventUtils.*;
import static org.junit.Assert.assertEquals;

public final class InterArrivalTimeStateTest {
    @Test
    public void InterArrivalTimeStateWorkflow() {
        // 1) create events with 5 seconds between events and add to state, set a buffer time of 20 seconds
        final InterArrivalTimeState state =
                new InterArrivalTimeState(now().getMillis(), 20L, new InterArrivalTime("node1"),
                        generateEvents(100, "node1", 5), null);
        // 2) move the buffer past the configured buffer time
        state.moveBufferIndex(now().plusSeconds(21).getMillis());
        // 3) get the buffered events without removing the state, should only contain 3 events because we only return
        //    half the allocated buffer time events
        final List<Event> events = state.bufferedInterArrivalTimeEvents();
        assertEquals(events.size(), 3);
    }
}
