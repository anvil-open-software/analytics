package com.dematic.labs.analytics.common.communication;

import java.util.concurrent.atomic.AtomicLong;

final class EventSequenceNumber {
    private static final AtomicLong SEQUENCE_NUMBER = new AtomicLong(1);

    private EventSequenceNumber() {
    }

    static long next() {
        return SEQUENCE_NUMBER.getAndIncrement();
    }
}