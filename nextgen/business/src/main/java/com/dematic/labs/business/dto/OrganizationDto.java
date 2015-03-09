package com.dematic.labs.business.dto;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class OrganizationDto {

    private String id;

    private String name;

    public OrganizationDto() {

    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setId(@NotNull String id) {
        this.id = id;
    }
    public void setName(@NotNull String name) {
        this.name = name;
    }

}
