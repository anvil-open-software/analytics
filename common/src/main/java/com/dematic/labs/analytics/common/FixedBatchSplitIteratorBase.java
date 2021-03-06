/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.common;

import static java.util.Spliterators.spliterator;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;

public abstract class FixedBatchSplitIteratorBase<T> implements Spliterator<T> {
    private final int batchSize;
    private final int characteristics;
    private long est;

    FixedBatchSplitIteratorBase(int characteristics, int batchSize, long est) {
        characteristics |= ORDERED;
        if ((characteristics & SIZED) != 0) characteristics |= SUBSIZED;
        this.characteristics = characteristics;
        this.batchSize = batchSize;
        this.est = est;
    }

    @Override
    public Spliterator<T> trySplit() {
        final HoldingConsumer<T> holder = new HoldingConsumer<>();
        if (!tryAdvance(holder)) return null;
        final Object[] a = new Object[batchSize];
        int j = 0;
        do a[j] = holder.value; while (++j < batchSize && tryAdvance(holder));
        if (est != Long.MAX_VALUE) est -= j;
        return spliterator(a, 0, j, characteristics());
    }

    @Override
    public Comparator<? super T> getComparator() {
        if (hasCharacteristics(SORTED)) return null;
        throw new IllegalStateException();
    }

    @Override
    public long estimateSize() {
        return est;
    }

    @Override
    public int characteristics() {
        return characteristics;
    }

    static final class HoldingConsumer<T> implements Consumer<T> {
        Object value;

        @Override
        public void accept(T value) {
            this.value = value;
        }
    }
}