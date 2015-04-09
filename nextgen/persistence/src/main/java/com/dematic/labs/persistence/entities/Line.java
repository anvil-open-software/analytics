package com.dematic.labs.persistence.entities;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.UUID;

@Entity(name = "line")
@Table(uniqueConstraints = @UniqueConstraint(name = "DocumentLine_U2", columnNames = {"tenantId", "name"}))
public class Line extends OwnedAssetEntity {

    @ManyToOne
    @JoinColumn(name = "documentId", nullable = false)
    private Document document;

    @Column(nullable = false)
    private int lineNo;

    @ManyToOne
    @JoinColumn(name = "itemMasterId", nullable = false)
    private ItemMaster itemMaster;

    @NotNull(message = "Line # may not be null")
    @Column(length = 60)
    private String name;

    @SuppressWarnings("UnusedDeclaration")
    Line() {
        super();
    }

    @SuppressWarnings("unused")
    Line(@Nonnull UUID tenantId) {
        super(tenantId);
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public void setName(@Nonnull String name) {
        this.name = name;
    }

    public int getLineNo() {
        return lineNo;
    }

    public void setLineNo(int lineNo) {
        this.lineNo = lineNo;
    }

    public void setDocument(@Nullable Document document) {
        this.document = document;
    }

    @Nullable
    public Document getDocument() {
        return document;
    }


    public ItemMaster getItemMaster() {
        return itemMaster;
    }

    public void setItemMaster(@Nonnull ItemMaster itemMaster) {
        this.itemMaster = itemMaster;
    }

}
