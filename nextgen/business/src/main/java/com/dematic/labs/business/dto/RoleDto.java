package com.dematic.labs.business.dto;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class RoleDto {

    private String id;
    private String name;


    public RoleDto() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
