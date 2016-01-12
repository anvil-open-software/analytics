package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.google.common.collect.Lists;
import org.joda.time.Seconds;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.dematic.labs.toolkit.communication.EventUtils.dateTime;

public final class InterArrivalTimeState implements Serializable {
    private final long startTimeInMs;
    private final long bufferTimeInsSconds;
    private final List<Event> events;
    private int bufferIndex;

    public InterArrivalTimeState(final long startTimeInMs, final long bufferTimeInSeconds, final List<Event> events) {
        this.startTimeInMs = startTimeInMs;
        this.bufferTimeInsSconds = Duration.ofSeconds(bufferTimeInSeconds).getSeconds();
        this.events = Lists.newLinkedList(events);
        bufferIndex = 0;
    }

    public boolean triggerInterArrivalTimeProcessing(final long timeInMs) {
        return Seconds.secondsBetween(dateTime(startTimeInMs), dateTime(timeInMs)).getSeconds() >= bufferTimeInsSconds;
    }

    public boolean removeInterArrivalTimeState() {
        // remove state if list is empty, i.e. all events have been processed and exceeded trigger time
        return events.isEmpty(); // todo: think about this && triggerInterArrivalTimeProcessing(Iterables.getLast(events).getTimestamp().getMillis());
    }

    public void addNewEvents(final List<Event> newEvents) {
        events.addAll(newEvents);
    }

    public void updateBuffer(final long timeInMs) {
        // move the index of the buffer, the elapse time has expired
        if (triggerInterArrivalTimeProcessing(timeInMs)) {
            // only process half of the buffer
            final long halfBuffer = bufferTimeInsSconds / 2; // i.e. 20 / 2 = 10

            // find the half buffer time
            final long halfBufferTime =
                    EventUtils.dateTime(timeInMs).minusSeconds(new Long(halfBuffer).intValue()).getMillis();

            // find the first event in the list that is half the buffer size and get the index
            final Optional<Event> first =
                    events.stream().filter(event -> event.getTimestamp().getMillis() > halfBufferTime).findFirst();

            // if no event has been found return all events
            if (!first.isPresent()) {
                bufferIndex = events.size();
            } else {
                // find the index of the first event in the buffer and assign the buffer index, exclusive
                bufferIndex = events.indexOf(first.get());
            }
        }
    }

    public List<Event> bufferedInterArrivalTimeEvents() {
        // todo: figure out event return event...move buffer....
        final List<Event> bufferedEvents = new ArrayList<>(this.events.subList(0, bufferIndex));
        // remove from the event list
        if (!bufferedEvents.isEmpty()) {
            events.removeAll(bufferedEvents);
        }
        return bufferedEvents;
    }
}
