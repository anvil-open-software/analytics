package com.dematic.labs.persistence.entities;

import com.dematic.labs.matchers.ConstraintViolationMatcher;
import com.dematic.labs.matchers.HibernateWrappedCauseMatcher;
import com.dematic.labs.persistence.JpaRule;
import com.dematic.labs.persistence.MySql;
import com.dematic.labs.picketlink.RealmSelector;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.picketlink.idm.model.basic.Realm;

import java.util.UUID;

import static junit.framework.TestCase.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocumentTest {

    private final UUID tenantId = UUID.randomUUID();
    private CrudService crudService;

    @Rule
    public JpaRule jpaRule = new JpaRule(new MySql(), "NewPersistenceUnit");

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        Realm realm = new Realm();
        realm.setId(tenantId.toString());
        realm.setName("dummy");
        RealmSelector realmSelector = mock(RealmSelector.class);
        when(realmSelector.select()).thenReturn(realm);
        crudService = new CrudService(jpaRule.getEntityManager(), realmSelector);
    }

    @Test
    public void testSave() {

        Document document = crudService.createNewOwnedAsset(Document.class);
        assertNull(document.getId());
        assertEquals(tenantId, document.getTenantId());

        document.setName("SO-0000001");

        crudService.create(document);
        assertNotNull(document.getId());

        jpaRule.changeTransaction();

        Document documentFromDb = crudService.findExisting(Document.class, document.getId());
        assertEquals(document.getId(), documentFromDb.getId());
        assertEquals("SO-0000001", documentFromDb.getName());
    }

    @Test
    public void testUpdate() {

        //create document
        Document documentFromDb;
        {
            Document document = crudService.createNewOwnedAsset(Document.class);
            assertNull(document.getId());

            document.setName("SO-0000001");

            crudService.create(document);
            assertNotNull(document.getId());

            jpaRule.changeTransaction();

            documentFromDb = crudService.findExisting(Document.class, document.getId());
            assertEquals(0, documentFromDb.getVersion());

        }

        //update name
        {

            documentFromDb.setName("PO-0000001");

            jpaRule.changeTransaction();

            Document updatedDocument = crudService.findExisting(Document.class, documentFromDb.getId());
            assertEquals("PO-0000001", updatedDocument.getName());
            assertEquals(1, updatedDocument.getVersion());
        }

    }

    @Test
    public void testUniqueWithinTenant() {

        //save first one
        {
            Document document = crudService.createNewOwnedAsset(Document.class);
            assertNull(document.getId());

            document.setName("SO-0000001");

            crudService.create(document);
            assertNotNull(document.getId());

            jpaRule.changeTransaction();
        }

        //attempt saving duplicate
        {
            Document document = crudService.createNewOwnedAsset(Document.class);
            assertNull(document.getId());

            document.setName("SO-0000001");

            expectedException.expectCause(new HibernateWrappedCauseMatcher(ConstraintViolationException.class,
                    MySQLIntegrityConstraintViolationException.class,
                    "Duplicate entry '.+' for key 'Document_U2'"));
            crudService.create(document);
        }
    }

    @Test
    public void testDuplicateAmongTenants() {

        UUID tenantAId = UUID.randomUUID();
        UUID tenantBId = UUID.randomUUID();

        //save first one
        {
            Document document = new Document(tenantAId);
            assertNull(document.getId());

            document.setName("SO-0000001");

            crudService.create(document);
            assertNotNull(document.getId());

            jpaRule.changeTransaction();
        }

        //save same name in different tenant
        {
            Document document = new Document(tenantBId);
            assertNull(document.getId());

            document.setName("SO-0000001");

            crudService.create(document);
            jpaRule.changeTransaction();
        }
    }

    @Test
    public void testConstraintViolations() {

        Document document = new Document();

        assertNull(document.getTenantId());

        expectedException.expect(new ConstraintViolationMatcher("Document Name may not be null",
                "Tenant ID may not be null"));
        crudService.create(document);
    }

}
