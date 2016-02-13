package com.dematic.labs.analytics.ingestion.sparks.drivers;

import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.joda.time.Seconds;

import java.io.Serializable;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.dematic.labs.toolkit.communication.EventUtils.dateTime;

public final class InterArrivalTimeState implements Serializable {
    private final long startTimeInMs;
    private final long bufferTimeInsSconds;
    private List<Event> events;
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
        return events.isEmpty();
    }

    public void addNewEvents(final List<Event> newEvents) {
        events.addAll(newEvents);
        events = Ordering.from(new Comparator<Event>() {
            @Override
            public int compare(Event event1, Event event2) {
                return event1.getTimestamp().compareTo(event2.getTimestamp());
            }
        }).sortedCopy(events);
    }

    public void moveBufferIndex(final long timeInMs) {
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
        final List<Event> copy = copy(events.subList(0, bufferIndex));
        bufferIndex = 0;
        return copy;
    }

    public List<Event> allInterArrivalTimeEvents() {
        return copy(events);
    }

    private static List<Event> copy(final List<Event> events) {
        final List<Event> copy = Lists.newArrayList(events);
        // remove from the original events and reset the index
        events.clear();
        return copy;
    }
}