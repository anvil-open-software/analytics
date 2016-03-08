package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.ingestion.sparks.tables.Bucket;
import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.dematic.labs.analytics.ingestion.sparks.tables.BucketUtils.addToBucket;
import static com.dematic.labs.analytics.ingestion.sparks.tables.BucketUtils.bucketTimeInSeconds;
import static com.dematic.labs.analytics.ingestion.sparks.tables.BucketUtils.bucketsToJson;
import static java.util.Collections.sort;

//todo: avg cycle time ? do we need it
public final class CycleTimeState implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CycleTimeState.class);

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

    public CycleTime createModel() {
        // calculate the CT and add to buckets, todo: parallel
        jobs.asMap().entrySet().stream().forEach(job -> {
            if (job.getValue().size() == 1) {
                //todo: figure out how to handle errors and time for how long to leave in memory
            } else {
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
}
