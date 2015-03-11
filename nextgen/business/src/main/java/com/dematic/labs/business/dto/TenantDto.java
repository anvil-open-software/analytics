package com.dematic.labs.business.dto;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TenantDto extends IdentifiableDto {

    private String name;

    @SuppressWarnings("UnusedDeclaration") //needed for jackson json
    public TenantDto() {
    }

    public String getName() {
        return name;
    }

    public void setName(@NotNull String username) {
        this.name = username;
    }

}
