package com.dematic.labs.analytics.ingestion.drivers;

import com.dematic.labs.analytics.ingestion.sparks.drivers.InterArrivalTimeState;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventUtils;
import org.junit.Test;

import java.util.List;

import static com.dematic.labs.toolkit.communication.EventUtils.*;

public final class InterArrivalTimeStateTest {
    @Test
    public void InterArrivalTimeStateWorkflow() {
        // 1) create events and state
        generateEvents(100, "node1", 5);
        final InterArrivalTimeState state =
                new InterArrivalTimeState(now().getMillis(), 20L, generateEvents(100, "node1", 5));
        state.updateBuffer(now().plusSeconds(21).getMillis());
        final List<Event> events = state.bufferedInterArrivalTimeEvents();
        final List<Event> event2 = state.bufferedInterArrivalTimeEvents();
        System.out.println();
    }
}
