package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

@SuppressWarnings("unused")
public final class Bucket implements Serializable {
    private final Set<Integer> pair = Sets.newLinkedHashSet();
    private long count;

    public Bucket(final int low, final int high, final long count) {
        pair.add(low);
        pair.add(high);
        this.count = count;
    }

    public Set<Integer> getPair() {
        return pair;
    }

    public int getLowerBoundry() {
        return Iterables.get(pair, 0);
    }

    public int getUpperBoundry() {
        return Iterables.get(pair, 1);
    }

    // inclusive/exclusive
    public boolean isWithinBucket(final long interArrivalTime) {
        return getLowerBoundry() <= interArrivalTime && getUpperBoundry() > interArrivalTime;
    }

    public long getCount() {
        return count;
    }

    public void incrementCount() {
        count++;
    }

    public void setCount(final long count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bucket that = (Bucket) o;
        return Objects.equals(pair, that.pair);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pair);
    }

    @Override
    public String toString() {
        return "Bucket{" +
                "pair=" + pair +
                ", count=" + count +
                '}';
    }
}

