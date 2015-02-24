package com.dematic.labs.business.dto;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TenantDto {

    private String id;

    private String name;

    @SuppressWarnings("UnusedDeclaration") //needed for jackson json
    public TenantDto() {
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
    public void setName(@NotNull String username) {
        this.name = username;
    }

}
