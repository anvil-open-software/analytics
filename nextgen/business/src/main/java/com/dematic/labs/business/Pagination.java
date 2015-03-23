package com.dematic.labs.business;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class Pagination {

    public static final int DEFAULT_LIMIT = 25;

    public static final Pagination DEFAULT = new Pagination(0, DEFAULT_LIMIT);
    @Min(value = 0, message = "Pagination offset must be positive")
    private final int offset;

    @Min(value = 0, message = "Pagination limit must be positive")
    @Max(value = 100, message = "Pagination limit may not exceed 100")
    private final int limit;

    public Pagination(int offset, int limit) {

        this.offset = offset;
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

}
