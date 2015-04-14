package com.dematic.labs.matchers;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HibernateWrappedCauseMatcher extends TypeSafeDiagnosingMatcher<Throwable> {

    private final Class<? extends Throwable> type, cause;
    private final String expectedMessage;

    public HibernateWrappedCauseMatcher(Class<? extends Throwable> type, Class<? extends Throwable> cause, String expectedMessage) {
        this.type = type;
        this.cause = cause;
        this.expectedMessage = expectedMessage;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" type ")
                .appendValue(type)
                .appendText(" cause ")
                .appendValue(cause)
                .appendText(" and a message ")
                .appendValue(expectedMessage);
    }

    @Override
    protected boolean matchesSafely(Throwable item, Description mismatchDescription) {
        if (!item.getClass().isAssignableFrom(type)) {
            mismatchDescription.appendText(" type:").appendText(type.getSimpleName());
            return false;
        }
        if (!item.getCause().getClass().isAssignableFrom(cause)) {
            mismatchDescription.appendText(" cause:").appendText(cause.getSimpleName());
            return false;
        }
        Pattern pattern = Pattern.compile(expectedMessage);
        Matcher matcher = pattern.matcher(item.getCause().getMessage());
        if (!matcher.find()) {
            mismatchDescription.appendText(" message:").appendText(item.getCause().getMessage());
            return false;
        }

        return true;
    }

}
