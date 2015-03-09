package com.dematic.labs.persistence.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import javax.validation.constraints.NotNull;
import java.util.UUID;

@Entity(name = "organization")
@Table(uniqueConstraints = @UniqueConstraint(name = "Organization_U2", columnNames = {"tenantId", "name"}))
public class Organization extends OwnedAssetEntity {

    @Column(length = 60)
    private String name;

    @SuppressWarnings("UnusedDeclaration")
    public Organization() {
        super();
    }

    public Organization(UUID tenantId) {
        super(tenantId);
    }

    @NotNull
    public String getName() {
        return name;
    }

    public void setName(@NotNull String name) {
        this.name = name;
    }
}
