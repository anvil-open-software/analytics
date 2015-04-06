package com.dematic.labs.persistence.entities;

import com.dematic.labs.matchers.ConstraintViolationMatcher;
import com.dematic.labs.matchers.HibernateWrappedCauseMatcher;
import com.dematic.labs.persistence.Derby;
import com.dematic.labs.persistence.JpaRule;
import com.dematic.labs.picketlink.RealmSelector;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.picketlink.idm.model.basic.Realm;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ItemMasterTest {

    private CrudService crudService;

    @Rule
    public JpaRule jpaRule = new JpaRule(new Derby(), "NewPersistenceUnit");

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        Realm realm = new Realm();
        realm.setId(UUID.randomUUID().toString());
        realm.setName("dummy");
        RealmSelector realmSelector = mock(RealmSelector.class);
        when(realmSelector.select()).thenReturn(realm);
        crudService = new CrudService(jpaRule.getEntityManager(), realmSelector);
    }

    @Test
    public void testSave() {

        UUID tenantId = UUID.randomUUID();

        ItemMaster itemMaster = new ItemMaster(tenantId);
        assertNull(itemMaster.getId());
        assertEquals(tenantId, itemMaster.getTenantId());

        itemMaster.setName("Fred");

        crudService.create(itemMaster);
        assertNotNull(itemMaster.getId());

        jpaRule.changeTransaction();

        ItemMaster itemMasterFromDb = crudService.findExisting(ItemMaster.class, itemMaster.getId());
        assertEquals(itemMaster.getId(), itemMasterFromDb.getId());
        assertEquals("Fred", itemMasterFromDb.getName());
    }

    @Test
    public void testUpdate() {

        //create itemMaster
        ItemMaster itemMasterFromDb;
        {
            UUID tenantId = UUID.randomUUID();

            ItemMaster itemMaster = new ItemMaster(tenantId);
            assertNull(itemMaster.getId());

            itemMaster.setName("Fred");

            crudService.create(itemMaster);
            assertNotNull(itemMaster.getId());

            jpaRule.changeTransaction();

            itemMasterFromDb = crudService.findExisting(ItemMaster.class, itemMaster.getId());
            assertEquals(0, itemMasterFromDb.getVersion());

        }

        //update name
        {

            itemMasterFromDb.setName("Jane");

            jpaRule.changeTransaction();

            ItemMaster updatedItemMaster = crudService.findExisting(ItemMaster.class, itemMasterFromDb.getId());
            assertEquals("Jane", updatedItemMaster.getName());
            assertEquals(1, updatedItemMaster.getVersion());
        }

    }

    @Test
    public void testUniqueWithinTenant() {

        UUID tenantId = UUID.randomUUID();

        //save first one
        {
            ItemMaster itemMaster = new ItemMaster(tenantId);
            assertNull(itemMaster.getId());

            itemMaster.setName("Fred");

            crudService.create(itemMaster);
            assertNotNull(itemMaster.getId());

            jpaRule.changeTransaction();
        }

        //attempt saving duplicate
        {
            ItemMaster itemMaster = new ItemMaster(tenantId);
            assertNull(itemMaster.getId());

            itemMaster.setName("Fred");

            expectedException.expectCause(new HibernateWrappedCauseMatcher(ConstraintViolationException.class,
                    SQLIntegrityConstraintViolationException.class,
                    "duplicate key value in a unique or primary key constraint or unique index identified by 'ITEMMASTER_U2'"));
            crudService.create(itemMaster);
        }
    }

    @Test
    public void testDuplicateAmongTenants() {

        UUID tenantAId = UUID.randomUUID();
        UUID tenantBId = UUID.randomUUID();

        //save first one
        {
            ItemMaster itemMaster = new ItemMaster(tenantAId);
            assertNull(itemMaster.getId());

            itemMaster.setName("Fred");

            crudService.create(itemMaster);
            assertNotNull(itemMaster.getId());

            jpaRule.changeTransaction();
        }

        //save same name in different tenant
        {
            ItemMaster itemMaster = new ItemMaster(tenantBId);
            assertNull(itemMaster.getId());

            itemMaster.setName("Fred");

            crudService.create(itemMaster);
            jpaRule.changeTransaction();
        }
    }

    @Test
    public void testConstraintViolations() {

        ItemMaster itemMaster = new ItemMaster();

        assertNull(itemMaster.getTenantId());

        expectedException.expect(new ConstraintViolationMatcher("Item Master Name may not be null", "Tenant ID may not be null"));
        crudService.create(itemMaster);
    }


}
