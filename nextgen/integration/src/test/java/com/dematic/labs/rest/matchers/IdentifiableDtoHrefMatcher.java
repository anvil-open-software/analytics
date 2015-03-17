package com.dematic.labs.rest.matchers;

import com.dematic.labs.business.dto.IdentifiableDto;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class IdentifiableDtoHrefMatcher<T extends IdentifiableDto> extends TypeSafeDiagnosingMatcher<T> {

    @Override
    public void describeTo(Description description) {
        description.appendText("dto href ends with dto id ");

    }

    @Override
    protected boolean matchesSafely(T identifiableDto, Description mismatchDescription) {
        if (identifiableDto.getHref() == null) {
            mismatchDescription.appendText(identifiableDto.getClass().getSimpleName())
                    .appendText(" href is null ");
            return false;
        } else if (!identifiableDto.getHref().endsWith(identifiableDto.getId())) {
            mismatchDescription.appendText("href [").appendText(identifiableDto.getHref())
                    .appendText("] and dto id [").appendText(identifiableDto.getId()).appendText("]");
            return false;
        }
        return true;
    }

}
