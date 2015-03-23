package com.dematic.labs.business.matchers;

import com.dematic.labs.persistence.query.QueryParameters;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class QueryParametersMatcher extends TypeSafeDiagnosingMatcher<QueryParameters> {

    private final QueryParameters queryParameters;
    public QueryParametersMatcher(QueryParameters queryParameters) {

        this.queryParameters = queryParameters;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" offset: " + queryParameters.getOffset())
                .appendText(" and limit: " + queryParameters.getLimit());

    }

    @Override
    protected boolean matchesSafely(QueryParameters item, Description mismatchDescription) {

        if (item.getOffset() != queryParameters.getOffset()) {
            mismatchDescription.appendText(" offset: " + item.getOffset());
            return false;
        }
        if (item.getLimit() != queryParameters.getLimit()) {
            mismatchDescription.appendText(" limit: " + item.getLimit());
            return false;
        }

        return true;
    }

}
