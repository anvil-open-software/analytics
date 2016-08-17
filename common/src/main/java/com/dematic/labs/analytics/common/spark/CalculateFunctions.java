package com.dematic.labs.analytics.common.spark;

import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@SuppressWarnings("unused")
public final class CalculateFunctions implements Serializable {
    private CalculateFunctions() {
    }

    // Lambda Functions
    public static final Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

    public static final Function2<List<Long>, Optional<Long>, Optional<Long>> COMPUTE_RUNNING_SUM
            = (nums, existing) -> {
        long sum = existing.orElse(0L);
        for (long i : nums) {
            sum += i;
        }
        return Optional.of(sum);
    };

    public static final Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>> SUM_AND_COUNT_REDUCER
            = (x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2());

    public static final Function2<List<Tuple2<Double, Long>>, Optional<Tuple2<Double, Long>>, Optional<Tuple2<Double, Long>>>
            COMPUTE_RUNNING_AVG = (sums, existing) -> {
        Tuple2<Double, Long> avgAndCount = existing.orElse(new Tuple2<>(0.0, 0L));

        for (final Tuple2<Double, Long> sumAndCount : sums) {
            final double avg = avgAndCount._1();
            final long avgCount = avgAndCount._2();

            final double sum = sumAndCount._1();
            final long sumCount = sumAndCount._2();

            final Long countTotal = avgCount + sumCount;
            final Double newAvg = ((avgCount * avg) + (sumCount * sum / sumCount)) / countTotal;

            avgAndCount = new Tuple2<>(newAvg, countTotal);
        }
        return Optional.of(avgAndCount);
    };
}
