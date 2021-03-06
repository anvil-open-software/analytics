/*
 * Copyright 2018 Dematic, Corp.
 * Licensed under the MIT Open Source License: https://opensource.org/licenses/MIT
 */

package com.dematic.labs.analytics.common;

import static java.util.stream.StreamSupport.stream;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public class FixedBatchSplitIterator<T> extends FixedBatchSplitIteratorBase<T> {
    private final Spliterator<T> spliterator;

    private FixedBatchSplitIterator(final Spliterator<T> toWrap, final int batchSize, final long est) {
        super(toWrap.characteristics(), batchSize, est);
        this.spliterator = toWrap;
    }

    private FixedBatchSplitIterator(final Spliterator<T> toWrap, int batchSize) {
        this(toWrap, batchSize, toWrap.estimateSize());
    }

    public static <T> Stream<T> withBatchSize(final Stream<T> in, int batchSize) {
        return stream(new FixedBatchSplitIterator<>(in.spliterator(), batchSize), true);
    }

    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        return spliterator.tryAdvance(action);
    }

    @Override
    public void forEachRemaining(final Consumer<? super T> action) {
        spliterator.forEachRemaining(action);
    }
}