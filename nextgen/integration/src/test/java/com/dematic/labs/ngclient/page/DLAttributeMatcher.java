package com.dematic.labs.ngclient.page;

import org.hamcrest.Description;
import org.openqa.selenium.WebElement;

import javax.annotation.Nonnull;

public enum DLAttributeMatcher {

    DISABLED(AttributeProperty.DISABLED, "disabled") {
        @Override
        public boolean matches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
            return attributeMatches(element, mismatchDescription);
        }
    };

    private final String property;
    private final String value;

    DLAttributeMatcher(@Nonnull String property, @Nonnull String value) {
        this.property = property;
        this.value = value;
    }

    @Nonnull
    public String getProperty() {
        return property;
    }

    @Nonnull
    public String getValue() {
        return value;
    }

    public abstract boolean matches(@Nonnull WebElement element, @Nonnull Description mismatchDescription);

    protected boolean attributeMatches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
        String propertyValue = element.getAttribute(getProperty());
        if (propertyValue == null || !propertyValue.equals(getValue())) {
            mismatchDescription.appendText(" Property:").appendText(getProperty())
                    .appendText(" value: ").appendText(propertyValue == null ? "null" : propertyValue);
            return false;
        }
        return true;
    }
}
