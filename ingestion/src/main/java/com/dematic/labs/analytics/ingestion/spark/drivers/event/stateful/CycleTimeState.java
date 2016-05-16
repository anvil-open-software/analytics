package com.dematic.labs.analytics.ingestion.spark.drivers.event.stateful;

import com.dematic.labs.analytics.ingestion.spark.tables.event.Bucket;
import com.dematic.labs.analytics.ingestion.spark.tables.event.CycleTime;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventType;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.dematic.labs.analytics.ingestion.spark.tables.event.BucketUtils.*;
import static java.util.Collections.sort;

public final class CycleTimeState implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CycleTimeState.class);
    private static final int TIME_OUT_IN_SECONDS = 300; // todo: make configurable, 5 minutes

    private final String nodeId;
    // keep UUID's to events that were not processed because of missing jobs
    private final Multimap<UUID, Event> jobs;
    private Set<Bucket> buckets;
    private Long jobCount;
    private Long errorStartCount;
    private Long errorEndCount;

    public CycleTimeState(final String initialNodeId, final Multimap<UUID, Event> initialJobs,
                          final Set<Bucket> initialBuckets, final Long initialJobCount,
                          final Long initialErrorStartCount, final Long initialErrorEndCount) {
        nodeId = initialNodeId;
        jobs = initialJobs;
        buckets = initialBuckets;
        jobCount = initialJobCount;
        errorStartCount = initialErrorStartCount;
        errorEndCount = initialErrorEndCount;
    }

    public void updateJobs(final Multimap<UUID, Event> newJobs) {
        jobs.putAll(newJobs);
    }

    public CycleTime createModel(final boolean stateTime) {
        final Multimap<UUID, Event> jobsCopy = HashMultimap.create(jobs);
        // calculate the CT and add to buckets
        jobsCopy.asMap().entrySet().stream().forEach(job -> {
            // driver state timeout
            if (stateTime && job.getValue().size() == 1) {
                // calculate jobs that did not have job pairs, that is, start and end event
                // remove from jobs
                final Collection<Event> errors = jobs.removeAll(job.getKey());
                // update the error count
                updateErrorCount(errors);
                LOGGER.error("CT: state timeout : >{}< did not have complete set of events >{}<", job.getKey(), errors);
            } // state did not timeout but job did
            else if (job.getValue().size() == 1 && jobTimeout(Iterables.getOnlyElement(job.getValue()))) {
                // calculate jobs that did not have job pairs, that is, start and end event and have timed out
                final Collection<Event> errors = jobs.removeAll(job.getKey());
                // update the error count
                updateErrorCount(errors);
                LOGGER.error("CT: job timeout : >{}< did not have complete set of events >{}<", job.getKey(), errors);
            } else {
                if (job.getValue().size() > 2) {
                    LOGGER.error("CT: Unexpected Error: duplicate jobs incorrect number of events >{}<", job.getValue());
                } else if (job.getValue().size() != 2) {
                    LOGGER.debug("CT: Unexpected Error: missing jobs incorrect number of events >{}<", job.getValue());
                } else {
                    // ensure orderd by start and finish event type
                    final List<Event> completedJobs = new ArrayList<>(job.getValue());
                    sort(completedJobs, (final Event e1, final Event e2) -> e1.getType().compareTo(e2.getType()));
                    // calculate completed job CT
                    final Event start = completedJobs.get(0);
                    final Event end = completedJobs.get(1);
                    // add to bucket
                    addToBucket(bucketTimeInSeconds(end.getTimestamp().getMillis() - start.getTimestamp().getMillis()),
                            buckets);
                    // remove competed job from jobs and update the job count
                    jobs.removeAll(job.getKey());
                    jobCount++;
                }
            }
        });

        return new CycleTime(nodeId, bucketsToJson(buckets), jobCount, errorStartCount, errorEndCount);
    }

    private void updateErrorCount(final Collection<Event> errors) {
        errors.stream().forEach(event -> {
            final EventType type = event.getType();
            if (EventType.END == type) {
                // update the start
                errorStartCount++;
            }
            if (EventType.START == type) {
                // update the end
                errorEndCount++;
            }
        });
    }

    @Override
    public String toString() {
        return "CycleTimeState{" +
                "nodeId='" + nodeId + '\'' +
                ", jobs=" + jobs +
                ", buckets=" + buckets +
                ", jobCount=" + jobCount +
                ", errorStartCount=" + errorStartCount +
                ", errorEndCount=" + errorEndCount +
                '}';
    }

    private static boolean jobTimeout(final Event event) {
        return Seconds.secondsBetween(event.getTimestamp(), EventUtils.now()).getSeconds() >= TIME_OUT_IN_SECONDS;
    }
}
