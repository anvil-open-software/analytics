package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.ingestion.sparks.tables.Bucket;
import com.dematic.labs.analytics.ingestion.sparks.tables.CycleTime;
import com.dematic.labs.toolkit.communication.Event;
import com.dematic.labs.toolkit.communication.EventUtils;
import com.google.common.base.Optional;
import com.google.common.collect.Multimap;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;

import static com.dematic.labs.analytics.ingestion.sparks.tables.BucketUtils.createCycleTimeBuckets;
import static com.dematic.labs.analytics.ingestion.sparks.tables.CycleTimeFactory.findCycleTimeByNodeId;
import static com.dematic.labs.toolkit.aws.Connections.getDynamoDBMapper;

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
                    LOGGER.debug("CT: node >{}< state timeout >{}<", nodeId, EventUtils.nowString());
                    // no state has been updated for timeout amount of time, that is, no events associated to the node
                    // has been updated within the configured timeout, calculate any error cases, uuid's without pairs
                    return Optional.of(cycleTimeState.createModel(true));
                } else {
                    // add new UUID grouping to the map
                    cycleTimeState.updateJobs(jobs.get());
                    state.update(cycleTimeState);
                }
            } else {
                // create and add the initial state, see if existing state in DB
                final CycleTime cycleTimeByNodeId = findCycleTimeByNodeId(nodeId,
                        getDynamoDBMapper(driverConfig.getDynamoDBEndpoint(), driverConfig.getDynamoPrefix()));
                final Long jobCount = cycleTimeByNodeId != null ? cycleTimeByNodeId.getJobCount() : 0L;
                final Set<Bucket> buckets = cycleTimeByNodeId != null ?
                        createCycleTimeBuckets(cycleTimeByNodeId.getBuckets()) :
                        createCycleTimeBuckets(asInt(driverConfig.getBucketIncrementer()),
                                asInt(driverConfig.getBucketSize()));

                cycleTimeState = new CycleTimeState(nodeId, jobs.get(), buckets, jobCount);
                LOGGER.debug("CT: node >{}< created state >{}<", nodeId, cycleTimeState);
                state.update(cycleTimeState);
            }
            return Optional.of(cycleTimeState.createModel(false));
        }
    }

    private static int asInt(final String intString) {
        return Integer.parseInt(intString);
    }
}

