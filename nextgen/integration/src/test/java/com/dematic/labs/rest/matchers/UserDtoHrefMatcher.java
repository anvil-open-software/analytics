package com.dematic.labs.rest.matchers;

import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import org.hamcrest.Description;
import org.hamcrest.core.Every;

public class UserDtoHrefMatcher extends IdentifiableDtoHrefMatcher<UserDto> {

    private final IdentifiableDtoHrefMatcher<TenantDto> tenantDtoHrefMatcher;
    private final Every<RoleDto> grantedRoleDtoIdentifiableDtoHrefMatcher;

    public UserDtoHrefMatcher() {
        this.tenantDtoHrefMatcher = new IdentifiableDtoHrefMatcher<>();
        grantedRoleDtoIdentifiableDtoHrefMatcher = new Every<>(new IdentifiableDtoHrefMatcher<>());
    }

    @Override
    public void describeTo(Description description) {
        super.describeTo(description);
        description.appendText("and tenant dto href ends with dto id ");
        description.appendText("and each granted role dto href ends with dto id ");
    }

    @Override
    protected boolean matchesSafely(UserDto userDto, Description mismatchDescription) {
        return super.matchesSafely(userDto, mismatchDescription)
                && tenantDtoHrefMatcher.matchesSafely(userDto.getTenantDto(), mismatchDescription)
                && grantedRoleDtoIdentifiableDtoHrefMatcher.matchesSafely(userDto.getGrantedRoles(), mismatchDescription);
    }

}
