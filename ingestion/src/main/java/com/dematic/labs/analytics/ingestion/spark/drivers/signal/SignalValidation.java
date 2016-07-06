package com.dematic.labs.analytics.ingestion.spark.drivers.signal;

import java.util.Objects;

public final class SignalValidation {
    public static final String TABLE_NAME = "signal_validation";

    public static String createTableCql(final String keyspace) {
        return String.format("CREATE TABLE if not exists %s.%s (" +
                " year int," +
                " count counter," +
                " PRIMARY KEY (year)" +
                " WITH CLUSTERING ORDER BY (year DESC);", keyspace, TABLE_NAME);
    }

    private int year;
    private Long count;

    public SignalValidation(final int year, final Long count) {
        this.year = year;
        this.count = count;
    }

    public int getYear() {
        return year;
    }

    public void setYear(final int year) {
        this.year = year;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(final Long count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SignalValidation that = (SignalValidation) o;
        return year == that.year &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, count);
    }

    @Override
    public String toString() {
        return "SignalValidation{" +
                "year=" + year +
                ", count=" + count +
                '}';
    }
}
