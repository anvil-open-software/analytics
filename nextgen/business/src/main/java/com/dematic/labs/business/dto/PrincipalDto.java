package com.dematic.labs.business.dto;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PrincipalDto {

    private String id;

    private String username;

    public PrincipalDto() {

    }

    public String getId() {
        return id;
    }

    public String getUsername() {
        return username;
    }

    public void setId(@NotNull String id) {
        this.id = id;
    }
    public void setUsername(@NotNull String username) {
        this.username = username;
    }

}
