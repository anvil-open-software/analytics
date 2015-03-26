package com.dematic.labs.business.matchers;

import com.dematic.labs.business.dto.CollectionDto;
import com.dematic.labs.business.dto.IdentifiableDto;
import com.dematic.labs.persistence.query.QueryParameters;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class CollectionDtoMatcher<I extends IdentifiableDto> extends TypeSafeDiagnosingMatcher<CollectionDto<I>> {

    private final QueryParametersMatcher queryParametersMatcher;
    private final int size;

    public CollectionDtoMatcher(QueryParameters queryParameters, int size) {

        this.queryParametersMatcher = new QueryParametersMatcher(queryParameters);
        this.size = size;
    }

    @Override
    public void describeTo(Description description) {
        queryParametersMatcher.describeTo(description);
        description.appendText(" collection size equals size: " + size);
    }

    @Override
    protected boolean matchesSafely(CollectionDto<I> item, Description mismatchDescription) {
        if (!queryParametersMatcher.matchesSafely(item.getQueryParameters(), mismatchDescription)) {
            return false;
        }
        if (item.getSize() != item.getItems().size()) {
            mismatchDescription.appendText(" dto size: " + item.getSize())
                    .appendText(" collection size: " + item.getItems().size());
            return false;
        }
        if (item.getSize() != size) {
            mismatchDescription.appendText(" dto size: " + item.getSize());
            return false;
        }
        return true;
    }

}
