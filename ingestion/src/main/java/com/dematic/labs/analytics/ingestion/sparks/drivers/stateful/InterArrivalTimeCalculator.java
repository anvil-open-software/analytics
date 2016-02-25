package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTime;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTimeBucket;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTimeBucket.toInterArrivalTimeBucket;
import static com.dematic.labs.toolkit.communication.EventUtils.dateTime;

public final class InterArrivalTimeCalculator {
    private static final Logger LOGGER = LoggerFactory.getLogger(InterArrivalTimeCalculator.class);

    private InterArrivalTimeCalculator() {
    }

    public static void computeInterArrivalTime(final InterArrivalTime interArrivalTime,
                                               final List<Event> events,
                                               final Long lastEventTime, final int avgInterArrivalTime) {
        if (events == null || events.isEmpty()) {
            return;
        }
        // create the buckets
        final List<InterArrivalTimeBucket> buckets = createBuckets(avgInterArrivalTime);

        // remove and count all errors, that is, events that should have been processed with the last batch
        final List<Event> eventsWithoutErrors = errorChecker(events, lastEventTime);
        // calculate errors
        long errorCount = events.size() - eventsWithoutErrors.size();

        if (errorCount > 0) {
            LOGGER.error("IAT: node >{}< : errorCount >{}< : events size >{}<", interArrivalTime.getNodeId(),
                    errorCount, events.size());
        }

        if (eventsWithoutErrors.size() > 1) {
            // 1) calculate the IAT between batches, if events have been processed already
            if (lastEventTime != null) {
                final long interArrivalTimeBetweenBatches =
                        interArrivalTimeBetweenBatches(lastEventTime, eventsWithoutErrors);

                if (interArrivalTimeBetweenBatches == -1) {
                    // error, just update the error count
                    errorCount = errorCount + 1;
                    LOGGER.error("IAT: event time between batches '{}' > event time '{}'", lastEventTime,
                            eventsWithoutErrors.get(0).getTimestamp().getMillis());
                } else {
                    // update the bucket
                    addToBucket(interArrivalTimeInSeconds(interArrivalTimeBetweenBatches), buckets);
                }
            }

            // 2) calculate IAT between events and populate buckets, add all inter arrival times to buckets
            final PeekingIterator<Event> eventPeekingIterator =
                    Iterators.peekingIterator(eventsWithoutErrors.iterator());
            // iterate through the list of events
            for (; eventPeekingIterator.hasNext(); ) {
                final Event current = eventPeekingIterator.next();
                if (eventPeekingIterator.hasNext()) {
                    // events r in order
                    final long interArrivalTimeValue =
                            eventPeekingIterator.peek().getTimestamp().getMillis() - current.getTimestamp().getMillis();
                    final long interArrivalTimeValueInSeconds = interArrivalTimeInSeconds(interArrivalTimeValue);
                    addToBucket(interArrivalTimeValueInSeconds, buckets);
                }
            }
        } else if (eventsWithoutErrors.size() == 1) {
            // only one event
            final Event singleEvent = eventsWithoutErrors.get(0);
            // calculate the inter-arrival time from the last event in dynamoDB
            if (lastEventTime == null) {
                // if only 1 event and either no last last event time, just log
                LOGGER.debug("IAT: no previous event to calculate IAT for event {} and nodeId >{}<",
                        singleEvent.toString(), interArrivalTime.getNodeId());
            } else if (interArrivalTimeBetweenBatches(lastEventTime, eventsWithoutErrors) == -1) {
                // last event time is >, then current event, just add to the error count
                errorCount = errorCount + 1;
                LOGGER.error("IAT: last event time '{}' > then current event time '{}' for node >{}<",
                        lastEventTime, eventsWithoutErrors.get(0).getTimestamp().getMillis(),
                        interArrivalTime.getNodeId());
            } else {
                final long interArrivalTimeBetweenBatches =
                        interArrivalTimeBetweenBatches(singleEvent.getTimestamp().getMillis(), eventsWithoutErrors);
                addToBucket(interArrivalTimeInSeconds(interArrivalTimeBetweenBatches), buckets);
            }
        } else {
            final String lastEventTimeString = lastEventTime != null ? dateTime(lastEventTime).toString() : null;
            // all errors
            LOGGER.error(String.format("IAT: all events for node >%s< within batch are errors - errorCount >%s< " +
                            "batchSize >%s< : last saved IAT >%s< and last batched event time >%s<",
                    interArrivalTime.getNodeId(), errorCount, events.size(), lastEventTimeString,
                    Iterators.getLast(events.iterator()).getTimestamp().toString()));
        }

        // set the computed error count
        final Long existingErrorCount = interArrivalTime.getErrorCount();
        interArrivalTime.setErrorCount(existingErrorCount + errorCount);
        // set the buckets
        final Set<String> existingBuckets = interArrivalTime.getBuckets();
        if (existingBuckets == null || existingBuckets.isEmpty()) {
            final Set<String> bucketsString = Sets.newHashSet();
            buckets.stream().forEach(bucket -> bucketsString.add(bucket.toJson()));
            interArrivalTime.setBuckets(bucketsString);
        } else {
            // add existing and new,
            final List<InterArrivalTimeBucket> updatedBuckets = Lists.newArrayList();
            interArrivalTime.getBuckets().stream().
                    forEach(bucket -> updatedBuckets.add(toInterArrivalTimeBucket(bucket)));

            buckets.stream().forEach(newBucket -> {
                final int bucketIndex = updatedBuckets.indexOf(newBucket);
                if (bucketIndex > -1) {
                    final InterArrivalTimeBucket existingInterArrivalTimeBucket = updatedBuckets.remove(bucketIndex);
                    // add the counts
                    final long existingCount = existingInterArrivalTimeBucket.getCount();
                    final long newCount = newBucket.getCount();
                    existingInterArrivalTimeBucket.setCount(existingCount + newCount);
                    updatedBuckets.add(existingInterArrivalTimeBucket);
                } else {
                    // new bucket
                    updatedBuckets.add(newBucket);
                }
            });

            final Set<String> bucketsString = Sets.newHashSet();
            updatedBuckets.stream().forEach(bucket -> bucketsString.add(bucket.toJson()));
            interArrivalTime.setBuckets(bucketsString);
        }
    }

    private static List<InterArrivalTimeBucket> createBuckets(final int avgInterArrivalTime) {
        final List<InterArrivalTimeBucket> buckets = Lists.newArrayList();
        // see https://docs.google.com/document/d/1J9mSW8EbxTwbsGGeZ7b8TVkF5lm8-bnjy59KpHCVlBA/edit# for specs
        for (int i = 0; i < avgInterArrivalTime * 2; i++) {
            final int low = i * avgInterArrivalTime / 5;
            final int high = (i + 1) * avgInterArrivalTime / 5;
            if (high > avgInterArrivalTime * 2) {
                // add the last bucket
                buckets.add(new InterArrivalTimeBucket(low, Integer.MAX_VALUE, 0L));
                break;
            }
            buckets.add(new InterArrivalTimeBucket(low, high, 0L));
        }
        return buckets;
    }

    private static void addToBucket(final long intervalTime, final List<InterArrivalTimeBucket> buckets) {
        final java.util.Optional<InterArrivalTimeBucket> first = buckets.stream()
                .filter(bucket -> bucket.isWithinBucket(intervalTime)).findFirst();
        if (!first.isPresent()) {
            throw new IllegalStateException(String.format("IAT: - Unexpected Error: intervalTime >%s<" +
                    " not contained within buckets >%s<", intervalTime, buckets));
        }
        final InterArrivalTimeBucket interArrivalTimeBucket = first.get();
        interArrivalTimeBucket.incrementCount();
    }

    private static List<Event> errorChecker(final List<Event> unprocessedEvents, final Long lastEventTime) {
        if (lastEventTime == null) {
            // nothing to check against
            return unprocessedEvents;
        }

        // last event is before new events, just return
        if (lastEventTime < unprocessedEvents.get(0).getTimestamp().getMillis()) {
            return unprocessedEvents;
        }
        // find the fist event in the list that is after the last processed event time
        final java.util.Optional<Event> firstUnprocessedEvent = unprocessedEvents.stream()
                .filter(event -> lastEventTime < event.getTimestamp().getMillis()).findFirst();

        if (firstUnprocessedEvent.isPresent()) {
            LOGGER.error("IAT: unprocessed events : first event time >{}< last event time >{}<",
                    dateTime(unprocessedEvents.get(0).getTimestamp().getMillis()), dateTime(lastEventTime));

            // remove from the list all events that should have been processed, these are errors
            return unprocessedEvents.subList(unprocessedEvents.indexOf(firstUnprocessedEvent.get()),
                    unprocessedEvents.size());
        }
        // all error, just return empty list
        return Collections.emptyList();
    }

    private static long interArrivalTimeBetweenBatches(final Long lastEventTime, final List<Event> events) {
        if (lastEventTime == null || events == null || events.isEmpty()) {
            return -1;
        }
        // events r in order, if lastEventTime is > then current event, this is an error, just return -1
        final long eventTime = events.get(0).getTimestamp().getMillis();
        return lastEventTime > eventTime ? -1 : eventTime - lastEventTime;
    }

    private static long interArrivalTimeInSeconds(final long interArrivalTimeInMs) {
        return interArrivalTimeInMs / 1000;
    }
}
