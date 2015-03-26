package com.dematic.labs.rest.helpers;

import com.dematic.labs.persistence.query.QueryParameters.ColumnSort;
import com.dematic.labs.persistence.entities.SortDirection;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import javax.annotation.Nonnull;

public class ColumnSortMatcher extends TypeSafeDiagnosingMatcher<ColumnSort> {
    private final String columnName;
    private final SortDirection sortDirection;

    public ColumnSortMatcher(@Nonnull String columnName, @Nonnull SortDirection sortDirection) {
        this.columnName = columnName;
        this.sortDirection = sortDirection;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("columnName [").appendText(columnName).appendText("] and sort direction [")
                .appendText(sortDirection.toString()).appendText("]");

    }
    @Override
    protected boolean matchesSafely(ColumnSort item, Description mismatchDescription) {
        if (!columnName.equals(item.getPropertyName())) {
            mismatchDescription.appendText( "propertyName [").appendText(item.getPropertyName()).appendText("]");
            return false;
        }
        if (!sortDirection.equals(item.getSortDirection())) {
            mismatchDescription.appendText( "sort direction [")
                    .appendText(item.getSortDirection().toString()).appendText("]");
            return false;
        }
        return true;
    }

}
