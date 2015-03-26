package com.dematic.labs.business.dto;

import javax.annotation.Nonnull;

public abstract class IdentifiableDto {

    private String id;

    private String uri;

    public String getId() {
        return id;
    }

    public void setId(@Nonnull String id) {
        this.id = id;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(@Nonnull String uri) {
        this.uri = uri;
    }

}
