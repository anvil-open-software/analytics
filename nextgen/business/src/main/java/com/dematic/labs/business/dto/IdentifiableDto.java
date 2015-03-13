package com.dematic.labs.business.dto;

import javax.validation.constraints.NotNull;

public class IdentifiableDto {

    private String id;
    private String href;

    public String getId() {
        return id;
    }

    public void setId(@NotNull String id) {
        this.id = id;
    }

    public String getHref() {
        return href;
    }

    public void setHref(String href) {
        this.href = href;
    }

}
