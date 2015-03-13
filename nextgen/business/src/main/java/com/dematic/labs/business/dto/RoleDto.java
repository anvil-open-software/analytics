package com.dematic.labs.business.dto;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class RoleDto extends IdentifiableDto {

    private String name;


    public RoleDto() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
