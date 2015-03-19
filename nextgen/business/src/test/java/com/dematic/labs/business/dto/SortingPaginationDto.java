package com.dematic.labs.business.dto;

import org.joda.time.LocalDate;

import javax.annotation.Nonnull;

public class SortingPaginationDto extends IdentifiableDto {

    private final String name;
    private final LocalDate birthday;

    public SortingPaginationDto(String name, LocalDate birthday) {
        this.name = name;
        this.birthday = birthday;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public LocalDate getBirthday() {
        return birthday;
    }

}
