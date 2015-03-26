package com.dematic.labs.rest.matchers;

import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import org.hamcrest.Description;
import org.hamcrest.core.Every;

public class UserDtoUriMatcher extends IdentifiableDtoUriMatcher<UserDto> {

    private final IdentifiableDtoUriMatcher<TenantDto> tenantDtoUriMatcher;
    private final Every<RoleDto> grantedRoleDtoIdentifiableDtoUriMatcher;

    public UserDtoUriMatcher() {
        this.tenantDtoUriMatcher = new IdentifiableDtoUriMatcher<>();
        grantedRoleDtoIdentifiableDtoUriMatcher = new Every<>(new IdentifiableDtoUriMatcher<>());
    }

    @Override
    public void describeTo(Description description) {
        super.describeTo(description);
        description.appendText("and tenant dto uri ends with dto id ");
        description.appendText("and each granted role dto uri ends with dto id ");
    }

    @Override
    protected boolean matchesSafely(UserDto userDto, Description mismatchDescription) {
        return super.matchesSafely(userDto, mismatchDescription)
                && tenantDtoUriMatcher.matchesSafely(userDto.getTenantDto(), mismatchDescription)
                && grantedRoleDtoIdentifiableDtoUriMatcher.matchesSafely(userDto.getGrantedRoles(), mismatchDescription);
    }

}
