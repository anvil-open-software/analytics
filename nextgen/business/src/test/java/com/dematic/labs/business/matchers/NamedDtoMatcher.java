package com.dematic.labs.business.matchers;

import com.dematic.labs.business.dto.NamedDto;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class NamedDtoMatcher<T extends NamedDto> extends TypeSafeDiagnosingMatcher<T> {

    private final String name;

    public NamedDtoMatcher(String name) {
        this.name = name;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" name [").appendText(name).appendText("] ");

    }
    @Override
    protected boolean matchesSafely(T nameDto, Description mismatchDescription) {
        if (!nameDto.getName().equals(name)) {
            mismatchDescription.appendText("Dto name [").appendText(nameDto.getName()).appendText("]");
            return false;
        }
        return true;
    }

}
