package com.dematic.labs.persistence.matchers;

import com.dematic.labs.persistence.entities.Line;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class LineMatcher extends TypeSafeDiagnosingMatcher<Line> {

    private final int lineNo;
    private final String name;

    public LineMatcher(int lineNo, String name) {
        this.lineNo = lineNo;
        this.name = name;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" lineNo:" + lineNo).appendText(" name:" + name);

    }
    @Override
    protected boolean matchesSafely(Line line, Description mismatchDescription) {
        //noinspection ConstantConditions
        if(line.getDocument() == null) {
            mismatchDescription.appendText(" unknown document");
            return false;
        }
        if (line.getLineNo() != lineNo) {
            mismatchDescription.appendText(" lineNo:" + line.getLineNo());
            return false;
        }
        if (!line.getName().equals(name)) {
            mismatchDescription.appendText(" name:" + line.getName());
            return false;
        }
        return true;
    }

}
