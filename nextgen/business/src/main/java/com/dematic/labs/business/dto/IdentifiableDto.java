package com.dematic.labs.business.dto;

import javax.annotation.Nonnull;

public abstract class IdentifiableDto {

    private String id;
    private String href;

    public String getId() {
        return id;
    }

    public void setId(@Nonnull String id) {
        this.id = id;
    }

    public String getHref() {
        return href;
    }

    public void setHref(@Nonnull String href) {
        this.href = href;
    }

}
