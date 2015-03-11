package com.dematic.labs.persistence.entities;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.validation.constraints.NotNull;
import java.util.UUID;

@MappedSuperclass
public abstract class OwnedAssetEntity extends IdentifiableEntity {

    @Column(length = 36)
    @NotNull(message = "Tenant ID may not be null")
    private String tenantId;

    OwnedAssetEntity(@Nonnull UUID tenantId) {
        this.tenantId = tenantId.toString();
    }

    OwnedAssetEntity() {

    }

    public UUID getTenantId() {
        if (tenantId == null) {
            return null;
        }
        return UUID.fromString(tenantId);
    }
}
