package com.dematic.labs.analytics.ingestion.drivers;

import com.dematic.labs.analytics.ingestion.sparks.drivers.InterArrivalTimeState;
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
                new InterArrivalTimeState(now().getMillis(), 20L, generateEvents(100, "node1", 5));
        // 2) move the buffer past the configured buffer time
        state.moveBufferIndex(now().plusSeconds(21).getMillis());
        // 3) get the buffered events without removing the state, shoul only contain 3 events because we only return
        //    half the allocated buffer time events
        final List<Event> events = state.bufferedInterArrivalTimeEvents(false);
        assertEquals(events.size(), 3);
        // 4) events were not cleared should still be able to get them
        final List<Event> events_2 = state.bufferedInterArrivalTimeEvents(true);
        assertEquals(events_2.size(), 3);
        // 4) events were cleared should be 0
        final List<Event> events_3 = state.bufferedInterArrivalTimeEvents(true);
        assertEquals(events_3.size(), 0);
    }
}
