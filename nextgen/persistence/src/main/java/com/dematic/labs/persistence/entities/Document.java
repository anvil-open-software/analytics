package com.dematic.labs.persistence.entities;

import javax.annotation.Nonnull;
import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.*;

@Entity(name = "document")
@Table(uniqueConstraints = @UniqueConstraint(name = "Document_U2", columnNames = {"tenantId", "name"}))
public class Document extends OwnedAssetEntity {

    @NotNull(message = "Document Name may not be null")
    @Column(length = 60)

    private String name;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, mappedBy = "document")
    @OrderColumn(name = "lineNo", nullable = false)
    List<Line> lines = new ArrayList<>();


    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
    @JoinColumn(name = "documentId", nullable = false)
    Set<DocumentOrganization> organizations = new HashSet<>();

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

    public boolean addOrganization(@Nonnull Organization organization, @Nonnull BusinessRole businessRole) {

        if (!organization.isBusinessRoleGranted(businessRole)) {
            throw new IllegalArgumentException(String.format("Organization [%s] " +
                            "cannot participate in document with role [%s] " +
                            "because Organization hasn't been assigned that role",
                    organization.getName(),
                    businessRole.toString()));
        } else if (!organization.isBusinessRoleActive(businessRole)) {
            throw new IllegalArgumentException(String.format("Organization [%s] " +
                            "cannot participate in document with role [%s] " +
                            "because this role isn't active for the Organization",
                    organization.getName(),
                    businessRole.toString()));
        }

        DocumentOrganization documentOrganization = new DocumentOrganization(organization, businessRole);

        return !organizationsContains(documentOrganization) && organizations.add(documentOrganization);
//        return organizations.add(documentOrganization);
    }

    //to avoid overriding equals and hashcode on DocumentOrganization
    private boolean organizationsContains(DocumentOrganization target) {
        for (DocumentOrganization element : organizations) {
            if (element.getOrganization().getId().equals(target.getOrganization().getId()) &&
                    element.getBusinessRole().equals(target.getBusinessRole())) {
                return true;
            }
        }
        return false;
    }

    public Set<DocumentOrganization> getOrganizations() {
        return Collections.unmodifiableSet(organizations);
    }

    public void clearOrganizations() {
        organizations.clear();
    }
}
