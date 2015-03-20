package com.dematic.labs.rest.matchers;

import com.dematic.labs.business.dto.IdentifiableDto;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class IdentifiableDtoUriMatcher<T extends IdentifiableDto> extends TypeSafeDiagnosingMatcher<T> {

    @Override
    public void describeTo(Description description) {
        description.appendText("dto uri ends with dto id ");

    }

    @Override
    protected boolean matchesSafely(T identifiableDto, Description mismatchDescription) {
        if (identifiableDto.getUri() == null) {
            mismatchDescription.appendText(identifiableDto.getClass().getSimpleName())
                    .appendText(" uri is null ");
            return false;
        } else if (!identifiableDto.getUri().endsWith(identifiableDto.getId())) {
            mismatchDescription.appendText("uri [").appendText(identifiableDto.getUri())
                    .appendText("] and dto id [").appendText(identifiableDto.getId()).appendText("]");
            return false;
        }
        return true;
    }

}
