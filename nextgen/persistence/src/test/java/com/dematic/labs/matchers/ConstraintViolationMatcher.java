package com.dematic.labs.matchers;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.core.IsEqual;

import javax.validation.ConstraintViolationException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class ConstraintViolationMatcher extends TypeSafeMatcher<ConstraintViolationException> {

    private final IsIterableContainingInAnyOrder<String> elementMatcher;

    public ConstraintViolationMatcher(String... expectedMessages) {

        elementMatcher = new IsIterableContainingInAnyOrder<>(Arrays.asList(expectedMessages).stream()
                .map(IsEqual::equalTo).collect(Collectors.toList()));
    }

    @Override
    protected boolean matchesSafely(ConstraintViolationException constraintViolationException) {

        Set<String> violations = constraintViolationException.getConstraintViolations().stream()
                .map(javax.validation.ConstraintViolation::getMessage).collect(Collectors.toSet());

        return elementMatcher.matches(violations);
    }

    @Override
    public void describeTo(Description description) {
        description.appendDescriptionOf(elementMatcher);
    }

}
