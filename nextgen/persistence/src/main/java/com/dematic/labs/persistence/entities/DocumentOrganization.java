package com.dematic.labs.persistence.entities;

import javax.annotation.Nonnull;
import javax.persistence.*;
import javax.validation.constraints.NotNull;

@Entity(name = "documentOrganization")
@Table(uniqueConstraints = @UniqueConstraint(name = "DocumentOrganization_U3", columnNames = {"documentId", "organizationId", "businessRole"}))
public class DocumentOrganization extends IdentifiableEntity {

    @ManyToOne
    @JoinColumn(name = "organizationId", nullable = false)
    private Organization organization;

    @Enumerated(EnumType.STRING)
    @Column(name = "businessRole", nullable = false, length = 40)
    @NotNull
    private BusinessRole businessRole;

    @SuppressWarnings("UnusedDeclaration")
    DocumentOrganization() {
        super();
    }

    public DocumentOrganization(@Nonnull Organization organization, @Nonnull BusinessRole businessRole) {
        this.organization = organization;
        this.businessRole = businessRole;
    }

    public Organization getOrganization() {
        return organization;
    }

    public void setOrganization(Organization organization) {
        this.organization = organization;
    }

    public BusinessRole getBusinessRole() {
        return businessRole;
    }

    public void setBusinessRole(BusinessRole businessRole) {
        this.businessRole = businessRole;
    }

/*
    @Override
    public int hashCode() {
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        hashCodeBuilder.append(getOrganization().getId())
                .append(getBusinessRole());

        return hashCodeBuilder.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof DocumentOrganization)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        DocumentOrganization documentOrganization = (DocumentOrganization) obj;

        if (getId() != null && documentOrganization.getId() != null) {
            return getId().equals(documentOrganization.getId());
        }

        return getOrganization().getId().equals(documentOrganization.getOrganization().getId()) &&
                getBusinessRole().equals(documentOrganization.getBusinessRole());

    }
*/

}
