package com.dematic.labs.analytics.common.communication;

import org.hamcrest.CoreMatchers;
import org.hamcrest.junit.MatcherAssert;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;


public final class EventTest {
    @Test
    public void convertEventToJson() throws IOException {
        // test event to json then to event
        final Event rawEvent = new Event(UUID.randomUUID(), EventSequenceNumber.next(), "Node-1", UUID.randomUUID(),
                EventType.UNKNOWN, DateTime.now(), "UnitTestGenerated", 1L);
        final String jsonEvent = EventUtils.eventToJson(rawEvent);
        final Event fromJson = EventUtils.jsonToEvent(jsonEvent);
        MatcherAssert.assertThat(rawEvent.getId(), CoreMatchers.is(fromJson.getId()));
        MatcherAssert.assertThat(rawEvent.getSequence(), CoreMatchers.is(fromJson.getSequence()));
        MatcherAssert.assertThat(rawEvent.getNodeId(), CoreMatchers.is(fromJson.getNodeId()));
        MatcherAssert.assertThat(rawEvent.getType(), CoreMatchers.is(fromJson.getType()));
        MatcherAssert.assertThat(rawEvent.getTimestamp(), CoreMatchers.is(fromJson.getTimestamp()));
        MatcherAssert.assertThat(rawEvent.getGeneratorId(), CoreMatchers.is(fromJson.getGeneratorId()));
        MatcherAssert.assertThat(rawEvent.getVersion(), CoreMatchers.is(fromJson.getVersion()));
    }

    @Test
    public void generateEvents() {
        final long numberOfEvents = 1000000;
        // test generation of events
        final List<Event> events = EventUtils.generateEvents(numberOfEvents, "Node-1");
        Assert.assertEquals(numberOfEvents, events.size());
    }
}
