package com.dematic.labs.persistence.entities;

import javax.annotation.Nonnull;
import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity(name = "document")
@Table(uniqueConstraints = @UniqueConstraint(name = "Document_U2", columnNames = {"tenantId", "name"}))
public class Document extends OwnedAssetEntity {

    @NotNull(message = "Document Name may not be null")
    @Column(length = 60)

    private String name;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, mappedBy = "document")
    @OrderColumn(name = "lineNo", nullable = false)
    List<Line> lines = new ArrayList<>();

    @SuppressWarnings("UnusedDeclaration")
    Document() {
        super();
    }

    Document(@Nonnull UUID tenantId) {
        super(tenantId);
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public void setName(@Nonnull String name) {
        this.name = name;
    }

    public void addLine(@Nonnull Line line) {
        line.setDocument(this);
        int index = lines.size();
        lines.add(line);
        line.setLineNo(index);
    }

    public void addLine(int index, Line line) {

        line.setDocument(this);

        //need to fetch the collection so that insertion is at correct point
        lines.size();

        lines.add(index, line);

        //need to adjust lineNo's above the insert point (for java consistency with db)
        for (int i = index; i<lines.size(); i++) {
            lines.get(i).setLineNo(i);
        }
    }

    public void removeLine(int index) {
        Line line = lines.get(index);
        line.setDocument(null);
        lines.remove(index);

        //need to adjust lineNo's above the remove point (for java consistency with db)
        for (int i = index; i<lines.size(); i++) {
            lines.get(i).setLineNo(i);
        }
    }
}
