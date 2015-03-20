package com.dematic.labs.business.dto;

import javax.annotation.Nonnull;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
public class OrganizationDto extends NamedDto {

    private List<OrganizationBusinessRoleDto> businessRoles = new ArrayList<>();

    public OrganizationDto() {

    }

    public OrganizationDto(@Nonnull String name) {
        setName(name);
    }

    public List<OrganizationBusinessRoleDto> getBusinessRoles() {
        return businessRoles;
    }

    public void setBusinessRoles(List<OrganizationBusinessRoleDto> businessRoles) {
        this.businessRoles = businessRoles;
    }

}
