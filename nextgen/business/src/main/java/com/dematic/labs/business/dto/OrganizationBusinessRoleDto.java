package com.dematic.labs.business.dto;

import com.dematic.labs.persistence.entities.BusinessRole;

import javax.annotation.Nonnull;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class OrganizationBusinessRoleDto {

    private String businessRole;
    private boolean active;

    public OrganizationBusinessRoleDto(@Nonnull BusinessRole businessRole, boolean active) {
        this.businessRole = businessRole.toString();
        this.active = active;
    }

    //needed for resteasy jaxb
    @SuppressWarnings("UnusedDeclaration")
    OrganizationBusinessRoleDto() {
    }

    public String getBusinessRole() {
        return businessRole;
    }

    public void setBusinessRole(@Nonnull String businessRole) {
        this.businessRole = businessRole;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public String toString() {
        return "OrganizationBusinessRoleDto{" +
                "businessRole='" + businessRole + '\'' +
                ", active=" + active +
                '}';
    }
}
