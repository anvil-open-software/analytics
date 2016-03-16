package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.ingestion.sparks.tables.Bucket;
import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.google.common.base.Optional;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;


public final class CycleTimeFunctions implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CycleTimeFunctions.class);

    private CycleTimeFunctions() {
    }

    public static final class createModel implements Function4<Time, String, Optional<Multimap<UUID, Event>>,
            State<CycleTimeState>, Optional<CycleTime>> {

        private final CycleTimeDriverConfig driverConfig;

        public createModel(final CycleTimeDriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public Optional<CycleTime> call(final Time time, final String nodeId, final Optional<Multimap<UUID, Event>> jobs,
                                        final State<CycleTimeState> state) throws Exception {
            final CycleTimeState cycleTimeState;
            if (state.exists()) {
                cycleTimeState = state.get();

                final boolean timingOut = state.isTimingOut();
                if (timingOut) {
                    LOGGER.info("CT: node >{}< state timeout >{}<", nodeId, EventUtils.nowString());
                    // no state has been updated for timeout amount of time, that is, no events associated to the node
                    // has been updated within the configured timeout, calculate any error cases, uuid's without pairs
                    return Optional.of(cycleTimeState.createModel(true));
                } else {
                    // add new UUID grouping to the map
                    cycleTimeState.updateJobs(jobs.get());
                    state.update(cycleTimeState);
                }
            } else {
                // create and add the initial state, todo: check the db for saved state
                cycleTimeState = new CycleTimeState(nodeId, jobs.get(),
                        createCycleTimeBuckets(asInt(driverConfig.getBucketIncrementer()),
                                asInt(driverConfig.getBucketSize())));
                LOGGER.info("CT: node >{}< created state >{}<", nodeId, cycleTimeState);
                state.update(cycleTimeState);
            }
            return Optional.of(cycleTimeState.createModel(false));
        }
    }

    private static int asInt(final String intString){
        return Integer.parseInt(intString);
    }

    public static Set<Bucket> createCycleTimeBuckets(final int bucketIncrement, final int numberOfBuckets) {
        // see https://docs.google.com/document/d/1J9mSW8EbxTwbsGGeZ7b8TVkF5lm8-bnjy59KpHCVlBA/edit# for specs
        // todo: did not follow specs, the numbers did not work out
        final Set<Bucket> buckets = Sets.newTreeSet(new Comparator<Bucket>() {
            @Override
            public int compare(final Bucket b1, final Bucket b2) {
                return Integer.compare(b1.getLowerBoundry(), b2.getLowerBoundry());
            }
        });
        for (int i = 0; i < numberOfBuckets; i++) {
            final int low = i * bucketIncrement;
            buckets.add(new Bucket(low, low + bucketIncrement, 0L));
        }
        // add the last bucket
        buckets.add(new Bucket(bucketIncrement * numberOfBuckets, Integer.MAX_VALUE, 0L));
        return buckets;
    }
}

