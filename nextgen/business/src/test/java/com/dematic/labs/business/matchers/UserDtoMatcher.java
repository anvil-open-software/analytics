package com.dematic.labs.business.matchers;

import com.dematic.labs.business.dto.UserDto;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import javax.annotation.Nonnull;

public class UserDtoMatcher extends TypeSafeDiagnosingMatcher<UserDto> {

    private final String loginName;
    private final String tenantName;

    public UserDtoMatcher(@Nonnull String loginName, @Nonnull String tenantName) {
        this.loginName = loginName;
        this.tenantName = tenantName;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" loginName [").appendText(loginName).appendText("] and tenantName")
                .appendText(tenantName).appendText("] ");

    }
    @Override
    protected boolean matchesSafely(UserDto userDto, Description mismatchDescription) {
        if (!userDto.getLoginName().equals(loginName)) {
            mismatchDescription.appendText("loginName [").appendText(userDto.getLoginName()).appendText("]");
            return false;
        }
        if (!userDto.getTenantDto().getName().equals(tenantName)) {
            mismatchDescription.appendText("tenantName [").appendText(userDto.getTenantDto().getName()).appendText("]");
            return false;
        }
        return true;
    }

}
