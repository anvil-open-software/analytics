package com.dematic.labs.persistence.entities;

import javax.annotation.Nonnull;
import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Entity(name = "organization")
@Table(uniqueConstraints = @UniqueConstraint(name = "Organization_U2", columnNames = {"tenantId", "name"}))
public class Organization extends OwnedAssetEntity {

    @NotNull(message = "Organization Name may not be null")
    @Column(length = 60)
    private String name;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
    @JoinColumn(name = "organizationId", nullable = false)
    @MapKeyEnumerated(EnumType.STRING)
    @MapKeyColumn(name = "businessRole")
    private Map<BusinessRole, OrganizationBusinessRole> businessRoles = new HashMap<>();

    @SuppressWarnings("UnusedDeclaration")
    protected Organization() {
        super();
    }


    Organization(@Nonnull UUID tenantId) {
        super(tenantId);
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public void setName(@Nonnull String name) {
        this.name = name;
    }

    @Nonnull
    public Map<BusinessRole, OrganizationBusinessRole> getBusinessRoles() {
        return Collections.unmodifiableMap(businessRoles);
    }

    public void addBusinessRole(@Nonnull BusinessRole businessRole, boolean active) {
        addBusinessRole(new OrganizationBusinessRole(businessRole, active));
    }

    public void addBusinessRole(@Nonnull OrganizationBusinessRole organizationBusinessRole) {
        businessRoles.put(organizationBusinessRole.getBusinessRole(), organizationBusinessRole);

    }
    public void removeBusinessRole(@Nonnull BusinessRole businessRole) {
        businessRoles.remove(businessRole);
    }

    public boolean isBusinessRoleGranted(@Nonnull BusinessRole businessRole) {
        return businessRoles.get(businessRole) != null;
    }

    public boolean isBusinessRoleActive(@Nonnull BusinessRole businessRole) {
        return isBusinessRoleGranted(businessRole) && businessRoles.get(businessRole).isActive();
    }
}
