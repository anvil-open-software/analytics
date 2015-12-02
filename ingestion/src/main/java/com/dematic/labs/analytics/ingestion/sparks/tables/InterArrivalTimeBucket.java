package com.dematic.labs.analytics.ingestion.sparks.tables;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

@SuppressWarnings("unused")
public final class InterArrivalTimeBucket implements Serializable {
    private final Set<Integer> pair = Sets.newHashSet();
    private long count;

    public InterArrivalTimeBucket(final int low, final int high, final long count) {
        pair.add(low);
        pair.add(high);
        this.count = count;
    }

    public Set<Integer> getPair() {
        return pair;
    }

    public int getLowerBoundry() {
        return Iterables.get(pair, 1);
    }

    public int getUpperBoundry() {
        return Iterables.get(pair, 2);
    }

    public long getCount() {
        return count;
    }

    public void setCount(final long count) {
        this.count = count;
    }

    public String toJson() {
        return "";
    }

    public static InterArrivalTimeBucket toInterArrivalTimeBucket(final String json) {
        return new InterArrivalTimeBucket(1, 1, 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InterArrivalTimeBucket that = (InterArrivalTimeBucket) o;
        return Objects.equals(pair, that.pair);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pair);
    }

    @Override
    public String toString() {
        return "InterArrivalTimeBucket{" +
                "pair=" + pair +
                ", count=" + count +
                '}';
    }
}

