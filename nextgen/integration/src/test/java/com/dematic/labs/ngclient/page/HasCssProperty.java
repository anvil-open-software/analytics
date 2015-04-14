package com.dematic.labs.ngclient.page;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.openqa.selenium.WebElement;

import java.util.EnumSet;

public class HasCssProperty extends TypeSafeDiagnosingMatcher<WebElement> {

    private final EnumSet<CssMatcher> cssMatcherEnumSet;

    public HasCssProperty(EnumSet<CssMatcher> cssMatcherEnumSet) {

        this.cssMatcherEnumSet = cssMatcherEnumSet;
    }

    @Override
    public void describeTo(Description description) {
        for (CssMatcher cssMatcher : cssMatcherEnumSet) {
            description.appendText(" Property:").appendText(cssMatcher.getProperty())
                    .appendText(" value:").appendText(cssMatcher.getValue());
        }

    }

    @Override
    protected boolean matchesSafely(WebElement element, Description mismatchDescription) {
        for (CssMatcher cssMatcher : cssMatcherEnumSet) {
            if (!cssMatcher.matches(element, mismatchDescription)) {
                return false;
            }
        }

        return true;
    }

}
