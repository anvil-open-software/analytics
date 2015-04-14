package com.dematic.labs.persistence.entities;

import com.dematic.labs.persistence.query.QueryParameters;
import com.dematic.labs.picketlink.RealmSelector;
import com.mysema.query.jpa.JPQLQuery;
import com.mysema.query.jpa.impl.JPAQuery;
import com.mysema.query.support.Expressions;
import com.mysema.query.types.*;
import org.picketlink.annotations.PicketLink;

import javax.annotation.Nonnull;
import javax.ejb.Stateless;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.validation.constraints.NotNull;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.picketlink.common.reflection.Reflections.findDeclaredConstructor;

@Stateless
public class CrudService {

    @PersistenceContext
    private EntityManager entityManager;

    @Inject
    RealmSelector realmSelector;

    @SuppressWarnings("UnusedDeclaration")
    public CrudService() {
    }

    public CrudService(@NotNull EntityManager entityManager, @NotNull RealmSelector realmSelector) {
        this.entityManager = entityManager;
        this.realmSelector = realmSelector;
    }

    @NotNull
    public <T> T create(T t) {
        entityManager.persist(t);
        entityManager.flush();
        entityManager.refresh(t);
        return t;
    }

    public <T extends OwnedAssetEntity> T createNewOwnedAsset(Class<T> ownedAssetClass) {

        //noinspection unchecked
        Constructor<T> expectedConstructor = (Constructor<T>) findDeclaredConstructor(ownedAssetClass, UUID.class);

        if (expectedConstructor == null) {
            throw new IllegalArgumentException("Owned Asset Subclass [" + ownedAssetClass.getName() +
                    "] must provide a constructor that accepts a UUID.");
        }

        try {
            return expectedConstructor.newInstance(UUID.fromString(realmSelector.select().getId()));
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("Reflection exception for [" + ownedAssetClass.getName() +
                    "]", e);
        }
    }

    @Nonnull
    public <T> T findExisting(Class<T> clazz, UUID id) {
        T rtnValue = entityManager.find(clazz, id.toString());
        if (rtnValue == null) {
            throw new IllegalArgumentException("Entity not found with id: " + id);
        }
        return rtnValue;
    }

    // inspections want @SafeVarargs and final on method which interferes with stateless bean
    @SuppressWarnings("unchecked")
    @Nonnull
    public <T extends OwnedAssetEntity> JPQLQuery createQuery(QueryParameters queryParameters, EntityPath<T>... sources) {
        JPQLQuery rtnValue = new JPAQuery(entityManager);

        rtnValue.from(sources);
        for (EntityPath<T> path : sources) {
            Path<String> tenantIdField = Expressions.path(String.class, path, "tenantId");
            Expression<String> tenantIdValue = Expressions.constant(realmSelector.select().getId());
            rtnValue.where(Expressions.predicate(Ops.EQ, tenantIdField, tenantIdValue));
        }

        if (!queryParameters.getOrderBy().isEmpty()) {
            List<OrderSpecifier<? extends Comparable>> orderSpecifierList = queryParameters.getOrderBy()
                    .stream()
                    .map(columnSort -> new OrderSpecifier<>(
                            columnSort.getOrder(), columnSort.getPath()))
                    .collect(Collectors.toList());


            rtnValue.orderBy(orderSpecifierList.toArray(new OrderSpecifier<?>[orderSpecifierList.size()]));
        }
        rtnValue.offset(queryParameters.getOffset()).limit(queryParameters.getLimit());

        return rtnValue;
    }

    /*
     * Since we are using JPAIdentityStore to store identity-related data, we must provide it with an EntityManager via a
     * producer method or field annotated with the @PicketLink qualifier.
     */
    @Produces
    @PicketLink
    public EntityManager getPicketLinkEntityManager() {
        return entityManager;
    }

}
