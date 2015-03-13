package com.dematic.labs.business.dto;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
public class OrganizationDto extends IdentifiableDto {

    private String name;

    private List<OrganizationBusinessRoleDto> businessRoles = new ArrayList<>();

    public OrganizationDto() {

    }

    public String getName() {
        return name;
    }

    public void setName(@NotNull String name) {
        this.name = name;
    }

    public List<OrganizationBusinessRoleDto> getBusinessRoles() {
        return businessRoles;
    }

    public void setBusinessRoles(List<OrganizationBusinessRoleDto> businessRoles) {
        this.businessRoles = businessRoles;
    }

}
