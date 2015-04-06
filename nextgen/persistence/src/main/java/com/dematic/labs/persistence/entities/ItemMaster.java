package com.dematic.labs.persistence.entities;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import javax.validation.constraints.NotNull;
import java.util.UUID;

@Entity(name = "itemMaster")
@Table(uniqueConstraints = @UniqueConstraint(name = "ItemMaster_U2", columnNames = {"tenantId", "name"}))
public class ItemMaster extends OwnedAssetEntity {

    @NotNull(message = "Item Master Name may not be null")
    @Column(length = 60)
    private String name;

    @SuppressWarnings("UnusedDeclaration")
    ItemMaster() {
        super();
    }

    ItemMaster(@Nonnull UUID tenantId) {
        super(tenantId);
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public void setName(@Nonnull String name) {
        this.name = name;
    }

}
