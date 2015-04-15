package com.dematic.labs.matchers;

import com.dematic.labs.persistence.entities.BusinessRole;
import com.dematic.labs.persistence.entities.DocumentOrganization;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.UUID;

public class DocumentOrganizationMatcher extends TypeSafeDiagnosingMatcher<DocumentOrganization> {

    private final UUID organizationId;
    private final BusinessRole businessRole;

    public DocumentOrganizationMatcher(UUID organizationId, BusinessRole businessRole) {

        this.businessRole = businessRole;
        this.organizationId = organizationId;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" organizationId:" + organizationId.toString())
                .appendText(" businessRole:" + businessRole.toString());
    }

    @Override
    protected boolean matchesSafely(DocumentOrganization item, Description mismatchDescription) {
        if (!item.getOrganization().getId().equals(organizationId)) {
            mismatchDescription.appendText(" organizationId:" + item.getOrganization().getId().toString());
            return false;
        }
        if (!item.getBusinessRole().equals(businessRole)) {
            mismatchDescription.appendText(" businessRole:" + item.getBusinessRole().toString());
            return false;
        }
        return true;
    }

}
