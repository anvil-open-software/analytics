package com.dematic.labs.persistence.entities;

import com.dematic.labs.matchers.DocumentOrganizationMatcher;
import com.dematic.labs.persistence.JpaRule;
import com.dematic.labs.persistence.MySql;
import com.dematic.labs.picketlink.RealmSelector;
import com.mysema.query.jpa.JPQLQuery;
import com.mysema.query.jpa.impl.JPAQuery;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.picketlink.idm.model.basic.Realm;

import java.util.List;
import java.util.UUID;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocumentOrganizationTest {

    private final UUID tenantId = UUID.randomUUID();
    private UUID organizationId1, organizationId2;
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

        //create organization
        {
            Organization organization = crudService.createNewOwnedAsset(Organization.class);
            organization.setName("ACME");
            organization.addBusinessRole(BusinessRole.CUSTOMER, true);
            organization.addBusinessRole(BusinessRole.CARRIER, false);
            crudService.create(organization);
            jpaRule.changeTransaction();
            organizationId1 = organization.getId();
        }

        //create 2nd organization
        {
            Organization organization = crudService.createNewOwnedAsset(Organization.class);
            organization.setName("ACE");
            organization.addBusinessRole(BusinessRole.SUPPLIER, true);
            organization.addBusinessRole(BusinessRole.OPERATOR, false);
            crudService.create(organization);
            jpaRule.changeTransaction();
            organizationId2 = organization.getId();
        }
    }

    @Test
    public void testAddOrganization() {

        Organization organization = crudService.findExisting(Organization.class, organizationId1);

        Document document = crudService.createNewOwnedAsset(Document.class);
        document.setName("SO-0000001");
        crudService.create(document);
        jpaRule.changeTransaction();

        document.addOrganization(organization, BusinessRole.CUSTOMER);

        jpaRule.changeTransaction();
        jpaRule.getEntityManager().clear();

        {
            Document documentFromDb = crudService.findExisting(Document.class, document.getId());

            assertThat(documentFromDb.getOrganizations(),
                    Matchers.contains(new DocumentOrganizationMatcher(organizationId1, BusinessRole.CUSTOMER)));
        }
    }

    @Test
    public void testAddOrganizationNonAssignedRole() {

        Organization organization = crudService.findExisting(Organization.class, organizationId1);

        Document document = crudService.createNewOwnedAsset(Document.class);
        document.setName("SO-0000001");
        crudService.create(document);
        jpaRule.changeTransaction();

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format("Organization [%s] " +
                        "cannot participate in document with role [%s] " +
                        "because Organization hasn't been assigned that role",
                organization.getName(),
                BusinessRole.SUPPLIER.toString()));
        document.addOrganization(organization, BusinessRole.SUPPLIER);

    }

    @Test
    public void testAddOrganizationNonActivedRole() {

        Organization organization = crudService.findExisting(Organization.class, organizationId1);

        Document document = crudService.createNewOwnedAsset(Document.class);
        document.setName("SO-0000001");
        crudService.create(document);
        jpaRule.changeTransaction();

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format("Organization [%s] " +
                        "cannot participate in document with role [%s] " +
                        "because this role isn't active for the Organization",
                organization.getName(),
                BusinessRole.CARRIER.toString()));
        document.addOrganization(organization, BusinessRole.CARRIER);

    }

    @Test
    public void testAddOrganizationDuplicate() {

        Organization organization = crudService.findExisting(Organization.class, organizationId1);

        Document document = crudService.createNewOwnedAsset(Document.class);
        document.setName("SO-0000001");
        crudService.create(document);
        jpaRule.changeTransaction();

        document.addOrganization(organization, BusinessRole.CUSTOMER);

        //two transient DocumentOrganizations are considered equal if both reference same org and same businessRole
        assertFalse(document.addOrganization(organization, BusinessRole.CUSTOMER));

        jpaRule.changeTransaction(); //unique constraint not triggered
        jpaRule.getEntityManager().clear();

        {
            Document documentFromDb = crudService.findExisting(Document.class, document.getId());

            /*
              a transient DocumentOrganization is considered equal to an attached one if both
              reference same org and same businessRole (i.e., Id is not considered in equality)
             */
            assertFalse(documentFromDb.addOrganization(organization, BusinessRole.CUSTOMER));
            jpaRule.changeTransaction();
        }
    }

    @Test
    public void testUpdateOrganization() {

        Organization organization1 = crudService.findExisting(Organization.class, organizationId1);

        Document document = crudService.createNewOwnedAsset(Document.class);
        document.setName("SO-0000001");
        crudService.create(document);
        jpaRule.changeTransaction();

        document.addOrganization(organization1, BusinessRole.CUSTOMER);

        jpaRule.changeTransaction();
        jpaRule.getEntityManager().clear();

        {
            Organization organization2 = crudService.findExisting(Organization.class, organizationId2);

            Document documentFromDb = crudService.findExisting(Document.class, document.getId());

            documentFromDb.clearOrganizations();
            assertTrue(documentFromDb.addOrganization(organization2, BusinessRole.SUPPLIER));

            jpaRule.changeTransaction();

            assertEquals(1, getDocuemntOrganizations().size());
        }
    }

    private List<DocumentOrganization> getDocuemntOrganizations() {

        JPQLQuery query = new JPAQuery(jpaRule.getEntityManager());
        return query.from(QDocumentOrganization.documentOrganization).list(QDocumentOrganization.documentOrganization);
    }
}
