package com.dematic.labs.persistence.entities;

import javax.annotation.Nonnull;
import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.UUID;

@Entity(name = "line")
@Table(uniqueConstraints = @UniqueConstraint(name = "DocumentLine_U2", columnNames = {"tenantId", "name"}))
public class Line extends OwnedAssetEntity {

    @ManyToOne
    @JoinColumn(name = "documentId", nullable = false)
    private Document document;

    @NotNull(message = "Line # may not be null")
    @Column(length = 60)
    private String name;

    @Column(nullable = false) //insertable = false, updatable = false,
    private int lineNo;

    @SuppressWarnings("UnusedDeclaration")
    Line() {
        super();
    }

    Line(@Nonnull UUID tenantId) {
        super(tenantId);
    }

    public Line(UUID tenantId, String name) {
        this(tenantId);
        setName(name);
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

    public void setDocument(@Nonnull Document document) {
        this.document = document;
    }

    @Nonnull
    public Document getDocument() {
        return document;
    }
}
