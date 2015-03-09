package com.dematic.labs.persistence.entities;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.validation.constraints.NotNull;
import java.util.UUID;

@MappedSuperclass
public abstract class OwnedAssetEntity extends IdentifiableEntity {

    @Column(length = 36)
    @NotNull
    private String tenantId;

    public OwnedAssetEntity(@Nonnull UUID tenantId) {
        this.tenantId = tenantId.toString();
    }

    public OwnedAssetEntity() {

    }

    public UUID getTenantId() {
        if (tenantId == null) {
            return null;
        }
        return UUID.fromString(tenantId);
    }
}
