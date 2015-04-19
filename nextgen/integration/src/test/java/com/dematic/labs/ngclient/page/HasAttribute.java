package com.dematic.labs.ngclient.page;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.openqa.selenium.WebElement;

import java.util.EnumSet;

public class HasAttribute extends TypeSafeDiagnosingMatcher<WebElement> {

    private final EnumSet<DLAttributeMatcher> dlAttributeMatcher;

    public HasAttribute(EnumSet<DLAttributeMatcher> dlAttributeMatcher) {

        this.dlAttributeMatcher = dlAttributeMatcher;
    }

    @Override
    public void describeTo(Description description) {
        for (DLAttributeMatcher thisMatcher : dlAttributeMatcher) {
            description.appendText(" Property:").appendText(thisMatcher.getProperty())
                    .appendText(" value:").appendText(thisMatcher.getValue());
        }

    }

    @Override
    protected boolean matchesSafely(WebElement element, Description mismatchDescription) {
        for (DLAttributeMatcher thisMatcher : dlAttributeMatcher) {
            if (!thisMatcher.matches(element, mismatchDescription)) {
                return false;
            }
        }

        return true;
    }
}
