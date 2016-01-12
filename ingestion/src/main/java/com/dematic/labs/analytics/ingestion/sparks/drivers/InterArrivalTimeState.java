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
    private final List<Event> bufferedEvents;
    private int bufferIndex;

    public InterArrivalTimeState(final long startTimeInMs, final long bufferTimeInSeconds, final List<Event> events) {
        this.startTimeInMs = startTimeInMs;
        this.bufferTimeInsSconds = Duration.ofSeconds(bufferTimeInSeconds).getSeconds();
        this.events = Lists.newLinkedList(events);
        this.bufferedEvents = Lists.newLinkedList();
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
            // add events to the buffer
            // todo: figure out if there is a better way to do this
            bufferedEvents.addAll(this.events.subList(0, bufferIndex));
            // remove from the event list
            if (!bufferedEvents.isEmpty()) {
                events.removeAll(bufferedEvents);
            }
        }
    }

    // todo: look into changing this structure and flow
    public List<Event> bufferedInterArrivalTimeEvents(final boolean clear) {
        final List<Event> copy = new ArrayList<>(bufferedEvents);
        if (clear) {
            bufferedEvents.clear();
        }
        return copy;
    }
}
