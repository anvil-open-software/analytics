package com.dematic.labs.business.matchers;

import com.dematic.labs.business.dto.RoleDto;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import javax.annotation.Nonnull;

public class RoleDtoMatcher extends TypeSafeDiagnosingMatcher<RoleDto> {

    private final String roleName;

    public RoleDtoMatcher(@Nonnull String roleName) {
        this.roleName = roleName;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" role [").appendText(roleName).appendText("] ");

    }
    @Override
    protected boolean matchesSafely(RoleDto roleDto, Description mismatchDescription) {
        if (!roleDto.getName().equals(roleName)) {
            mismatchDescription.appendText("RoleDto name [").appendText(roleDto.getName()).appendText("]");
            return false;
        }
        return true;
    }

}
