package com.dematic.labs.business.dto;

import javax.annotation.Nonnull;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Pagination {

    public static final int DEFAULT_LIMIT = 25;
    //needed for default values in rest layer (must be true constant)
    public static final String DEFAULT_LIMIT_AS_STRING = "25";
    public static final int MAX_LIMIT = 100;

    public static final Pagination DEFAULT = new Pagination(0, DEFAULT_LIMIT);

    @Min(value = 0, message = "Pagination offset must be positive")
    private final int offset;

    @Min(value = 0, message = "Pagination limit must be positive")
    @Max(value = MAX_LIMIT, message = "Pagination limit may not exceed " + MAX_LIMIT)
    private final int limit;

    private final List<ColumnSort> orderBy = new ArrayList<>();

    public Pagination(int offset, int limit) {

        this.offset = offset;
        this.limit = limit;
    }

    public Pagination(int offset, int limit, @Nonnull List<ColumnSort> orderBy) {

        this(offset, limit);
        this.orderBy.addAll(orderBy);
    }

    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

    public List<ColumnSort> getOrderBy() {
        return Collections.unmodifiableList(orderBy);
    }

    public static class ColumnSort {
        private final String propertyName;
        private final SortDirection sortDirection;

        public ColumnSort(@Nonnull String propertyName, @Nonnull SortDirection sortDirection) {
            this.propertyName = propertyName;
            this.sortDirection = sortDirection;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public SortDirection getSortDirection() {
            return sortDirection;
        }

    }
}
