package com.dematic.labs.analytics.ingestion.drivers.stateful;

import com.dematic.labs.analytics.ingestion.sparks.drivers.stateful.CycleTimeState;
import com.dematic.labs.analytics.ingestion.sparks.tables.Bucket;
import com.dematic.labs.analytics.ingestion.sparks.tables.BucketUtils;
import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public final class CycleTimeStateTest {
    @Test
    public void CycleTimeStateWorkflow() {
        final String nodeId = "CycleTimeStateWorkflow";
        // create events with a jobId to calculate CT
        final UUID jobId = UUID.randomUUID();
        // generate pair of events with 5 secs between events
        final List<Event> firstPair = EventUtils.generateCycleTimeEvents(2, nodeId, jobId, 5);
        final Set<Bucket> buckets = BucketUtils.createCycleTimeBuckets(5, 10);
        final Multimap<UUID, Event> jobs = HashMultimap.create();
        jobs.putAll(jobId, firstPair);
        final CycleTimeState cycleTimeState = new CycleTimeState(nodeId, jobs, buckets);
        final CycleTime model = cycleTimeState.createModel();
        // ensure job count == 1
        Assert.assertEquals(1, model.getJobCount().intValue());
    }
}
