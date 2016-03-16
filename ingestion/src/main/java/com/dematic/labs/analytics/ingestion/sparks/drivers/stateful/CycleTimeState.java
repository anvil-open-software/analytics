package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.ingestion.sparks.tables.Bucket;
import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;
import com.dematic.labs.toolkit.communication.Event;
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

import static com.dematic.labs.analytics.ingestion.sparks.tables.BucketUtils.*;
import static java.util.Collections.sort;

public final class CycleTimeState implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CycleTimeState.class);
    private static final int TIME_OUT_IN_SECONDS = 180; // todo: make configurable

    private final String nodeId;
    // keep UUID's to events that were not processed because of missing jobs
    private final Multimap<UUID, Event> jobs;
    private Set<Bucket> buckets;
    private Long jobCount;

    public CycleTimeState(final String nodeId, final Multimap<UUID, Event> initialJobs,
                          final Set<Bucket> initialBuckets) {
        this.nodeId = nodeId;
        this.jobs = initialJobs;
        this.buckets = initialBuckets;
        jobCount = 0L;
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
                // todo: for now, just log, need to store either in bucket or db table
                // calculate jobs that did not have job pairs, that is, start and end event
                // remove from jobs
                final Collection<Event> errors = jobs.removeAll(job.getKey());
                LOGGER.error("CT: state timeout : >{}< did not have complete set of events >{}<", job.getKey(), errors);
            } // state did not timeout but job did
            else if (job.getValue().size() == 1 && jobTimeout(Iterables.getOnlyElement(job.getValue()))) {
                // calculate jobs that did not have job pairs, that is, start and end event and have timed out
                final Collection<Event> errors = jobs.removeAll(job.getKey());
                LOGGER.error("CT: job timeout : >{}< did not have complete set of events >{}<", job.getKey(), errors);
            } else { //todo: better errors
                // ensure orderd by start and finish event type
                final List<Event> completedJobs = new ArrayList<>(job.getValue());
                sort(completedJobs, (final Event e1, final Event e2) -> e1.getType().compareTo(e2.getType()));
                // calculate completed job CT
                if (completedJobs.size() != 2) {
                    LOGGER.error("CT: Unexpected Error: completed jobs incorrect number of events >{}<", completedJobs);
                } else {
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
        return new CycleTime(nodeId, bucketsToJson(buckets), jobCount);
    }

    @Override
    public String toString() {
        return "CycleTimeState{" +
                "nodeId='" + nodeId + '\'' +
                ", jobs=" + jobs +
                ", buckets=" + buckets +
                ", jobCount=" + jobCount +
                '}';
    }

    private static boolean jobTimeout(final Event event) {
        return Seconds.secondsBetween(event.getTimestamp(), EventUtils.now()).getSeconds() >= TIME_OUT_IN_SECONDS;
    }
}
