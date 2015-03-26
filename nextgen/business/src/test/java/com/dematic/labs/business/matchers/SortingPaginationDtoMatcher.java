package com.dematic.labs.business.matchers;

import com.dematic.labs.business.dto.SortingPaginationDto;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class SortingPaginationDtoMatcher extends TypeSafeDiagnosingMatcher<SortingPaginationDto> {

    private final SortingPaginationDto sortingPaginationDto;
    public SortingPaginationDtoMatcher(SortingPaginationDto sortingPaginationDto) {

        this.sortingPaginationDto = sortingPaginationDto;
    }

    @Override
    public void describeTo(Description description) {

        description.appendText(" name: " + sortingPaginationDto.getName())
                .appendText(" birthday: " + sortingPaginationDto.getBirthday());
    }

    @Override
    protected boolean matchesSafely(SortingPaginationDto item, Description mismatchDescription) {
        if (!item.getName().equals(sortingPaginationDto.getName())) {
            mismatchDescription.appendText(" name: " + item.getName());
            return false;
        }
        if (!item.getBirthday().equals(sortingPaginationDto.getBirthday())) {
            mismatchDescription.appendText(" birthday: " + item.getBirthday());
            return false;
        }
        return true;
    }

}
