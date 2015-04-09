package com.dematic.labs.persistence.entities;

import com.dematic.labs.matchers.ConstraintViolationMatcher;
import com.dematic.labs.matchers.HibernateWrappedCauseMatcher;
import com.dematic.labs.persistence.JpaRule;
import com.dematic.labs.persistence.MySql;
import com.dematic.labs.persistence.matchers.LineMatcher;
import com.dematic.labs.persistence.query.QueryParameters;
import com.dematic.labs.persistence.query.QueryParametersHelper;
import com.dematic.labs.persistence.query.SortDirection;
import com.dematic.labs.picketlink.RealmSelector;
import com.mysema.query.jpa.JPQLQuery;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.picketlink.idm.model.basic.Realm;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static junit.framework.TestCase.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocumentTest {

    private final UUID tenantId = UUID.randomUUID();
    private CrudService crudService;
    private UUID itemMasterId1, itemMasterId2;

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

        //create item master
        {
            ItemMaster itemMaster = crudService.createNewOwnedAsset(ItemMaster.class);
            itemMaster.setName("BB-1");
            crudService.create(itemMaster);
            jpaRule.changeTransaction();
            itemMasterId1 = itemMaster.getId();
        }
        //create 2nd item master
        {
            ItemMaster itemMaster = crudService.createNewOwnedAsset(ItemMaster.class);
            itemMaster.setName("BB-2");
            crudService.create(itemMaster);
            jpaRule.changeTransaction();
            itemMasterId2 = itemMaster.getId();
        }
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

    @Test
    public void testAddRemoveLines() {

        //create document
        final Document originalDocument;
        {

            Document documentToSave = crudService.createNewOwnedAsset(Document.class);
            assertNull(documentToSave.getId());

            documentToSave.setName("SO-0000001");

            crudService.create(documentToSave);
            assertNotNull(documentToSave.getId());

            jpaRule.changeTransaction();

            originalDocument = crudService.findExisting(Document.class, documentToSave.getId());
        }

        //add lines
        {
            originalDocument.addLine(createLine(itemMasterId2, "second"));
            originalDocument.addLine(0, createLine(itemMasterId1, "first"));
            originalDocument.addLine(createLine(itemMasterId1, "third"));
            jpaRule.changeTransaction();

            {
                //these lines are the one's just created, i.e. testing for java consistency
                List<Line> lines = getLines(originalDocument.getId());
                assertThat(lines, IsIterableContainingInOrder.contains(new LineMatcher(0, itemMasterId1, "first"),
                        new LineMatcher(1, itemMasterId2, "second"),
                        new LineMatcher(2, itemMasterId1, "third")));
            }

            jpaRule.getEntityManager().clear();

            {
                //these lines are created by jpa, i.e. testing for db consistency
                List<Line> lines = getLines(originalDocument.getId());
                assertThat(lines, IsIterableContainingInOrder.contains(new LineMatcher(0, itemMasterId1, "first"),
                        new LineMatcher(1, itemMasterId2, "second"),
                        new LineMatcher(2, itemMasterId1, "third")));
            }

        }

        //add more lines
        {
            Document document = crudService.findExisting(Document.class, originalDocument.getId());
            document.addLine(0, createLine(itemMasterId1, "zero"));
            document.addLine(createLine(itemMasterId1, "forth"));

            jpaRule.changeTransaction();
            {
                //these lines are the one's just created, i.e. testing for java consistency
                List<Line> lines = getLines(document.getId());
                assertThat(lines, IsIterableContainingInOrder.contains(new LineMatcher(0, itemMasterId1, "zero"),
                        new LineMatcher(1, itemMasterId1, "first"),
                        new LineMatcher(2, itemMasterId2, "second"),
                        new LineMatcher(3, itemMasterId1, "third"),
                        new LineMatcher(4, itemMasterId1, "forth")));
            }

            jpaRule.getEntityManager().clear();

            {
                //these lines are created by jpa, i.e. testing for db consistency
                List<Line> lines = getLines(document.getId());
                assertThat(lines, IsIterableContainingInOrder.contains(new LineMatcher(0, itemMasterId1, "zero"),
                        new LineMatcher(1, itemMasterId1, "first"),
                        new LineMatcher(2, itemMasterId2, "second"),
                        new LineMatcher(3, itemMasterId1, "third"),
                        new LineMatcher(4, itemMasterId1, "forth")));
            }
        }

        //remove lines
        {
            Document document = crudService.findExisting(Document.class, originalDocument.getId());
            document.removeLine(2);

            jpaRule.changeTransaction();
            {
                //these lines are the one's just created, i.e. testing for java consistency
                List<Line> lines = getLines(document.getId());
                assertThat(lines, IsIterableContainingInOrder.contains(new LineMatcher(0, itemMasterId1, "zero"),
                        new LineMatcher(1, itemMasterId1, "first"),
                        new LineMatcher(2, itemMasterId1, "third"),
                        new LineMatcher(3, itemMasterId1, "forth")));
            }

            jpaRule.getEntityManager().clear();

            {
                //these lines are created by jpa, i.e. testing for db consistency
                List<Line> lines = getLines(document.getId());
                assertThat(lines, IsIterableContainingInOrder.contains(new LineMatcher(0, itemMasterId1, "zero"),
                        new LineMatcher(1, itemMasterId1, "first"),
                        new LineMatcher(2, itemMasterId1, "third"),
                        new LineMatcher(3, itemMasterId1, "forth")));
            }

            //itemMaster2 (of line 2) should still be in db
            assertNotNull(crudService.findExisting(ItemMaster.class, itemMasterId2));
        }


    }

    @Test
    public void testGetLinesWithMultipleDocumentsPresent() {

        final Document document1, document2;

        //create first document
        {
            Document documentToSave = crudService.createNewOwnedAsset(Document.class);
            assertNull(documentToSave.getId());

            documentToSave.setName("SO-0000001");

            crudService.create(documentToSave);
            assertNotNull(documentToSave.getId());

            jpaRule.changeTransaction();

            document1 = crudService.findExisting(Document.class, documentToSave.getId());
            document1.addLine(createLine(itemMasterId1, "second"));
            document1.addLine(0, createLine(itemMasterId1, "first"));
            document1.addLine(createLine(itemMasterId1, "third"));
            jpaRule.changeTransaction();
        }

        //create second document
        {
            Document documentToSave = crudService.createNewOwnedAsset(Document.class);
            assertNull(documentToSave.getId());

            documentToSave.setName("SO-0000002");

            crudService.create(documentToSave);
            assertNotNull(documentToSave.getId());

            jpaRule.changeTransaction();

            document2 = crudService.findExisting(Document.class, documentToSave.getId());
            document2.addLine(createLine(itemMasterId1, "fifth"));
            document2.addLine(0, createLine(itemMasterId1, "forth"));
            document2.addLine(createLine(itemMasterId1, "six"));
            jpaRule.changeTransaction();
        }

        jpaRule.getEntityManager().clear();

        //get document1 lines
        {
            List<Line> lines = getLines(document1.getId());
            assertThat(lines, IsIterableContainingInOrder.contains(new LineMatcher(0, itemMasterId1, "first"),
                    new LineMatcher(1, itemMasterId1, "second"),
                    new LineMatcher(2, itemMasterId1, "third")));
        }

    }

    private Line createLine(UUID itemMasterId, String name) {
        Line rtnValue = crudService.createNewOwnedAsset(Line.class);
        ItemMaster itemMaster = crudService.findExisting(ItemMaster.class, itemMasterId);
        rtnValue.setItemMaster(itemMaster);
        rtnValue.setName(name);
        return rtnValue;
    }

    private List<Line> getLines(UUID documentId) {
        List<QueryParameters.ColumnSort> columnSortList = new ArrayList<>();
        columnSortList.add(new QueryParameters.ColumnSort("lineNo", SortDirection.ASC));
        QueryParameters queryParameters = new QueryParameters(0, QueryParameters.DEFAULT_LIMIT, columnSortList);
        QueryParametersHelper.convertPropertyStringsToQueryPaths(queryParameters, QLine.line);

        JPQLQuery query = crudService.createQuery(queryParameters, QLine.line);
        query.where(QLine.line.document.id.eq(documentId.toString()));
        return query.list(QLine.line);
    }
}
