package com.dematic.labs.persistence;

import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.validation.constraints.NotNull;
import java.util.UUID;

@Stateless
public class CrudService {

    @PersistenceContext
    private EntityManager entityManager;

    @SuppressWarnings("UnusedDeclaration")
    public CrudService() {
    }

    public CrudService(@NotNull EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @NotNull
    public <T> T create(T t) {
        entityManager.persist(t);
        entityManager.flush();
        entityManager.refresh(t);
        return t;
    }

    @NotNull
    public <T> T findExisting(UUID id, Class<T> clazz) {
        T rtnValue = entityManager.find(clazz, id.toString());
        if (rtnValue == null) {
            throw new IllegalArgumentException("Entity not found with id: " + id);
        }
        return rtnValue;
    }

    @NotNull
    public EntityManager getEntityManager() {
        return entityManager;
    }
}
