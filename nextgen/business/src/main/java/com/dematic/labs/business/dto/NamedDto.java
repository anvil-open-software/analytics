package com.dematic.labs.business.dto;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

public abstract class NamedDto extends IdentifiableDto {

    @Size(min = 1, message = "Name may not be blank")
    private String name;

    public String getName() {
        return name;
    }

    public void setName(@NotNull String username) {
        this.name = username;
    }
}
