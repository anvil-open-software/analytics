package com.dematic.labs.analytics.ingestion.sparks.drivers.stateful;

import com.dematic.labs.analytics.common.spark.DriverConfig;
import com.dematic.labs.analytics.ingestion.sparks.tables.InterArrivalTime;
import com.dematic.labs.toolkit.communication.Event;
import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import java.util.List;

import static com.dematic.labs.analytics.ingestion.sparks.drivers.stateful.InterArrivalTimeCalculator.computeInterArrivalTime;
import static java.lang.Integer.valueOf;

public final class InterArrivalTimeFunctions {
    private InterArrivalTimeFunctions() {
    }

    public static final class EventByNodeFunction implements Function4<Time, String,
            Optional<List<Event>>, State<InterArrivalTimeState>,
            Optional<InterArrivalTime>> {
        private final DriverConfig driverConfig;

        public EventByNodeFunction(final DriverConfig driverConfig) {
            this.driverConfig = driverConfig;
        }

        @Override
        public Optional<InterArrivalTime> call(final Time time, final String nodeId,
                                               final Optional<List<Event>> events,
                                               final State<InterArrivalTimeState> state) throws Exception {
            // keeps the buffered events
            final InterArrivalTimeState interArrivalTimeState;
            if (state.exists()) {
                // get existing events
                interArrivalTimeState = state.get();

                final boolean timingOut = state.isTimingOut();
                if (timingOut) {
                    // no state has been updated for timeout amount of time, if any events are in the buffer just
                    // return all of them
                    return Optional.of(calculatedInterArrivalTime(interArrivalTimeState, false,
                            driverConfig.getMediumInterArrivalTime()));
                }

                // determine if we should remove state
                if (interArrivalTimeState.stateExpired()) {
                    state.remove();
                } else {
                    // add new events to state
                    interArrivalTimeState.addNewEvents(events.get());
                    interArrivalTimeState.moveBufferIndex(time.milliseconds());
                    state.update(interArrivalTimeState);
                }
            } else {
                // add the initial state
                interArrivalTimeState = new InterArrivalTimeState(time.milliseconds(),
                        Long.valueOf(driverConfig.getBufferTime()), new InterArrivalTime(nodeId), events.get(), null);
                state.update(interArrivalTimeState);
            }
            return Optional.of(calculatedInterArrivalTime(interArrivalTimeState, true,
                    driverConfig.getMediumInterArrivalTime()));
        }
    }

    private static InterArrivalTime calculatedInterArrivalTime(final InterArrivalTimeState interArrivalTimeState,
                                                               boolean bufferedEvents,
                                                               final String mediumInterArrivalTime) {
        final InterArrivalTime interArrivalTime = interArrivalTimeState.getInterArrivalTime();
        final List<Event> events = bufferedEvents ? interArrivalTimeState.bufferedInterArrivalTimeEvents() :
                interArrivalTimeState.allInterArrivalTimeEvents();
        computeInterArrivalTime(interArrivalTime, events, interArrivalTimeState.getLastEventTime(),
                valueOf(mediumInterArrivalTime));
        return interArrivalTime;
    }
}
