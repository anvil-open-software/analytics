package com.dematic.labs.ngclient.page;

import org.hamcrest.Description;
import org.openqa.selenium.WebElement;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;

public enum CssMatcher {

    THIN(CssProperty.WIDTH, "1px") {
        @Override
        public boolean matches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
            return borderMatches(element, mismatchDescription);
        }
    },
    THICK(CssProperty.WIDTH, "3px") {
        @Override
        public boolean matches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
            return borderMatches(element, mismatchDescription);
        }
    },

    BLUE(CssProperty.COLOR, "rgba(102, 175, 233, 1)") {
        @Override
        public boolean matches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
            return borderMatches(element, mismatchDescription);
        }
    },
    GOLD(CssProperty.COLOR, "rgba(255, 215, 0, 1)") {
        @Override
        public boolean matches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
            return borderMatches(element, mismatchDescription);
        }
    },
    GREY(CssProperty.COLOR, "rgba(204, 204, 204, 1)") {
        @Override
        public boolean matches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
            return borderMatches(element, mismatchDescription);
        }
    },
    RED(CssProperty.COLOR, "rgba(255, 0, 0, 1)") {
        @Override
        public boolean matches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
            return borderMatches(element, mismatchDescription);
        }
    },

    DARK_BLUE_BACKGROUND(CssProperty.BACKGROUND_COLOR, "rgba(51, 122, 183, 1)") {
        @Override
        public boolean matches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
            return backgroundColorMatches(element, mismatchDescription);
        }
    },
    DARK_GREY_BACKGROUND(CssProperty.BACKGROUND_COLOR, "rgba(169, 169, 169, 1)") {
        @Override
        public boolean matches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
            return backgroundColorMatches(element, mismatchDescription);
        }
    },
    LIGHT_GREY_BACKGROUND(CssProperty.BACKGROUND_COLOR, "rgba(230, 230, 230, 1)") {
        @Override
        public boolean matches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
            return backgroundColorMatches(element, mismatchDescription);
        }
    };

    private static List<String> sides = Arrays.asList("top", "bottom", "left", "right");

    private final String property;
    private final String value;

    CssMatcher(@Nonnull String property, @Nonnull String value) {
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

    protected boolean borderMatches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
        for (String side : sides) {
            String property = "border-" + side + "-" + getProperty();
            String propertyValue = element.getCssValue(property);
            if (!propertyValue.equals(getValue())) {
                mismatchDescription.appendText(" Property:").appendText(property)
                        .appendText(" value: ").appendText(propertyValue);
                return false;
            }
        }
        return true;
    }

    protected boolean backgroundColorMatches(@Nonnull WebElement element, @Nonnull Description mismatchDescription) {
        String propertyValue = element.getCssValue(getProperty());
        if (!propertyValue.equals(getValue())) {
            mismatchDescription.appendText(" Property:").appendText(getProperty())
                    .appendText(" value: ").appendText(propertyValue);
            return false;
        }
        return true;
    }

}
