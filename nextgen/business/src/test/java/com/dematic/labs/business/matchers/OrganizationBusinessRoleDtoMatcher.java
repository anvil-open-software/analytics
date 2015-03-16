package com.dematic.labs.business.matchers;

import com.dematic.labs.business.dto.OrganizationBusinessRoleDto;
import com.dematic.labs.persistence.entities.BusinessRole;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import javax.annotation.Nonnull;

public class OrganizationBusinessRoleDtoMatcher extends TypeSafeDiagnosingMatcher<OrganizationBusinessRoleDto> {

    private final String businessRole;
    private final boolean active;

    public OrganizationBusinessRoleDtoMatcher(@Nonnull BusinessRole businessRole, boolean active) {
        this.businessRole = businessRole.toString();
        this.active = active;
    }

    public OrganizationBusinessRoleDtoMatcher(OrganizationBusinessRoleDto organizationBusinessRoleDto) {
        this.businessRole = organizationBusinessRoleDto.getBusinessRole();
        this.active = organizationBusinessRoleDto.isActive();
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" business role [").appendText(businessRole)
                .appendText("] and active: ").appendValue(active);

    }

    @Override
    protected boolean matchesSafely(OrganizationBusinessRoleDto organizationBusinessRole, Description mismatchDescription) {
        if (!organizationBusinessRole.getBusinessRole().equals(businessRole)) {
            mismatchDescription.appendText(" BusinessRole [").appendText(organizationBusinessRole.getBusinessRole())
                    .appendText("] ");
            return false;
        }
        if (!organizationBusinessRole.isActive() == active) {
            mismatchDescription.appendText(" Active [").appendValue(active)
                    .appendText("] ");
            return false;
        }
        return true;
    }

    public static OrganizationBusinessRoleDtoMatcher equalTo(OrganizationBusinessRoleDto organizationBusinessRoleDto) {
        return new OrganizationBusinessRoleDtoMatcher(organizationBusinessRoleDto);
    }

}
