package com.dematic.labs.persistence;

import org.junit.rules.ExternalResource;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import javax.validation.constraints.NotNull;

public class JpaRule extends ExternalResource {

    @NotNull
    private final String persistenceUnitName;

    @NotNull
    private final DataSourceDefinition dataSourceDefinition;

    private EntityManager entityManager;
    private EntityManagerFactory emf;

    @SuppressWarnings("UnusedDeclaration")
    public JpaRule(@NotNull String persistenceUnitName) {
        this.persistenceUnitName = persistenceUnitName;
        dataSourceDefinition = new EmbeddedH2();
    }

    public JpaRule(@NotNull DataSourceDefinition databaseProperties, @NotNull String persistenceUnitName) {
        this.persistenceUnitName = persistenceUnitName;
        this.dataSourceDefinition = databaseProperties;
    }

    @Override
    public void before() {
        emf = Persistence.createEntityManagerFactory(persistenceUnitName, dataSourceDefinition.getProperties());
        entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
    }

    @Override
    public void after() {
        entityManager.close();
        emf.close();
    }

    public @NotNull EntityManager getEntityManager() {
        return entityManager;
    }

    public void changeTransaction() {
        EntityTransaction trx = entityManager.getTransaction();
        try {
            trx.commit();
            trx.begin();
        } catch (RuntimeException e) {
            if (trx != null && trx.isActive()) {
                trx.rollback();
            }
            throw e;
        }

    }
}
