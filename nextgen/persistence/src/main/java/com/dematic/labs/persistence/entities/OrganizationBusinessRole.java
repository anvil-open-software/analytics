package com.dematic.labs.persistence.entities;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.validation.constraints.NotNull;

@Entity
public class OrganizationBusinessRole extends IdentifiableEntity {

    @Enumerated(EnumType.STRING)
    @Column(name = "businessRole", nullable = false, length = 40)
    @NotNull
    private BusinessRole businessRole;

    @Column
    private boolean active;

    OrganizationBusinessRole() {
    }

    public OrganizationBusinessRole(@Nonnull BusinessRole businessRole, boolean active) {
        this.businessRole = businessRole;
        this.active = active;
    }

    @Nonnull
    public BusinessRole getBusinessRole() {
        return businessRole;
    }

    public boolean isActive() {
        return active;
    }

}
