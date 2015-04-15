package com.dematic.labs.persistence.entities;

import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;
import java.util.UUID;

@MappedSuperclass
public abstract class IdentifiableEntity {

    @Id
    @GeneratedValue(generator="system-uuid")
    @GenericGenerator(name="system-uuid", strategy = "uuid2")
    @Column(length = 36)
    private String id;

    @SuppressWarnings("unused")
    @Version
    private int version;

    public UUID getId() {
        if (id == null) {
            return null;
        }
        return UUID.fromString(id);
    }

    public int getVersion() {
        return version;
    }
}
