package com.dematic.labs.persistence.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mysema.query.types.Order;
import com.mysema.query.types.Path;

import javax.annotation.Nonnull;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@XmlRootElement
public class QueryParameters {

    public static final int DEFAULT_LIMIT = 25;
    //needed for default values in rest layer (must be true constant)
    public static final String DEFAULT_LIMIT_AS_STRING = "25";
    public static final int MAX_LIMIT = 100;

    public static final QueryParameters DEFAULT = new QueryParameters(0, DEFAULT_LIMIT);

    @Min(value = 0, message = "Pagination offset must be positive")
    private int offset;

    @Min(value = 0, message = "Pagination limit must be positive")
    @Max(value = MAX_LIMIT, message = "Pagination limit may not exceed " + MAX_LIMIT)
    private int limit;

    @Valid
    private List<ColumnSort> orderBy = new ArrayList<>();

    public QueryParameters(int offset, int limit) {

        this.offset = offset;
        this.limit = limit;
    }

    public QueryParameters(int offset, int limit, @Nonnull List<ColumnSort> orderBy) {

        this(offset, limit);
        this.orderBy.addAll(orderBy);
    }

    public QueryParameters() { //needed by jackson
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


    public void setOffset(int offset) { //needed by jackson
        this.offset = offset;
    }

    public void setLimit(int limit) { //needed by jackson
        this.limit = limit;
    }

    @SuppressWarnings("UnusedDeclaration") //needed by jackson
    public void setOrderBy(List<ColumnSort> orderBy) {
        this.orderBy = orderBy;
    }

    public static class ColumnSort {

        @Size(min = 1, message = "Sort Column name may not be empty")
        private String propertyName;

        @NotNull
        private SortDirection sortDirection;

        @JsonIgnore
        private Path<? extends Comparable> path;

        @JsonIgnore
        private Order order;

        public ColumnSort(@Nonnull String propertyName, @Nonnull SortDirection sortDirection) {
            this.propertyName = propertyName;
            this.sortDirection = sortDirection;
        }

        @SuppressWarnings("UnusedDeclaration") //needed by jackson
        public ColumnSort() {
        }

        public String getPropertyName() {
            return propertyName;
        }

        public SortDirection getSortDirection() {
            return sortDirection;
        }

        public void setQueryDslPath(@Nonnull Path<? extends Comparable> path) {
            this.path = path;
        }

        public void setQueryDslOrder(@Nonnull Order order) {
            this.order = order;
        }

        public Order getOrder() {
            return order;
        }

        public Path<? extends Comparable> getPath() {
            return path;
        }

        @SuppressWarnings("UnusedDeclaration") //needed by jackson
        public void setPropertyName(String propertyName) {
            this.propertyName = propertyName;
        }

        @SuppressWarnings("UnusedDeclaration") //needed by jackson
        public void setSortDirection(SortDirection sortDirection) {
            this.sortDirection = sortDirection;
        }

    }
}
