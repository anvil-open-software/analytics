package com.dematic.labs.persistence.entities;

import com.dematic.labs.matchers.ConstraintViolationMatcher;
import com.dematic.labs.matchers.HibernateWrappedCauseMatcher;
import com.dematic.labs.persistence.EmbeddedH2;
import com.dematic.labs.persistence.JpaRule;
import com.dematic.labs.persistence.query.QueryParameters;
import com.dematic.labs.persistence.query.QueryParametersHelper;
import com.dematic.labs.persistence.query.SortDirection;
import com.dematic.labs.picketlink.RealmSelector;
import org.h2.jdbc.JdbcSQLException;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.picketlink.idm.model.basic.Realm;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OrganizationTest {

    private final UUID tenantId = UUID.randomUUID();
    private CrudService crudService;

    @Rule
    public JpaRule jpaRule = new JpaRule(new EmbeddedH2(), "NewPersistenceUnit");

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

        Organization organization = crudService.createNewOwnedAsset(Organization.class);
        assertNull(organization.getId());
        assertEquals(tenantId, organization.getTenantId());

        organization.setName("Fred");
        organization.addBusinessRole(BusinessRole.CUSTOMER, true);
        organization.addBusinessRole(BusinessRole.CARRIER, false);

        crudService.create(organization);
        assertNotNull(organization.getId());

        jpaRule.changeTransaction();

        Organization organizationFromDb = crudService.findExisting(Organization.class, organization.getId());
        assertEquals(organization.getId(), organizationFromDb.getId());
        assertEquals("Fred", organizationFromDb.getName());
        assertThat(organizationFromDb.getBusinessRoles(), hasEntry(equalTo(BusinessRole.CUSTOMER)
                , new OrganizationBusinessRoleMatcher(true)));
        assertThat(organizationFromDb.getBusinessRoles(), hasEntry(equalTo(BusinessRole.CARRIER)
                , new OrganizationBusinessRoleMatcher(false)));
        assertEquals(2, organizationFromDb.getBusinessRoles().size());
    }

    @Test
    public void testUpdate() {

        //create organization
        Organization organizationFromDb;
        {
            Organization organization = crudService.createNewOwnedAsset(Organization.class);
            assertNull(organization.getId());

            organization.setName("Fred");
            organization.addBusinessRole(BusinessRole.CUSTOMER, true);
            organization.addBusinessRole(BusinessRole.CARRIER, false);

            crudService.create(organization);
            assertNotNull(organization.getId());

            jpaRule.changeTransaction();

            organizationFromDb = crudService.findExisting(Organization.class, organization.getId());

        }

        //update name and business roles
        {

            organizationFromDb.setName("Jane");
            organizationFromDb.removeBusinessRole(BusinessRole.CARRIER);
            organizationFromDb.removeBusinessRole(BusinessRole.CUSTOMER);
            organizationFromDb.addBusinessRole(BusinessRole.OPERATOR, true);
            organizationFromDb.addBusinessRole(BusinessRole.SUPPLIER, false);

            jpaRule.changeTransaction();

            Organization updatedOrganization = crudService.findExisting(Organization.class, organizationFromDb.getId());
            assertEquals("Jane", updatedOrganization.getName());
            assertThat(updatedOrganization.getBusinessRoles(), hasEntry(equalTo(BusinessRole.OPERATOR)
                    , new OrganizationBusinessRoleMatcher(true)));
            assertThat(updatedOrganization.getBusinessRoles(), hasEntry(equalTo(BusinessRole.SUPPLIER)
                    , new OrganizationBusinessRoleMatcher(false)));
            assertEquals(2, updatedOrganization.getBusinessRoles().size());
        }

    }

    @Test
    public void testUniqueWithinTenant() {

        //save first one
        {
            Organization organization = crudService.createNewOwnedAsset(Organization.class);
            assertNull(organization.getId());

            organization.setName("Fred");

            crudService.create(organization);
            assertNotNull(organization.getId());

            jpaRule.changeTransaction();
        }

        //attempt saving duplicate
        {
            Organization organization = crudService.createNewOwnedAsset(Organization.class);
            assertNull(organization.getId());

            organization.setName("Fred");

            expectedException.expectCause(new HibernateWrappedCauseMatcher(ConstraintViolationException.class,
                    JdbcSQLException.class,
                    "Unique index or primary key violation: \"ORGANIZATION_U2"));
            crudService.create(organization);
        }

    }

    @Test
    public void testDuplicateAmongTenants() {

        UUID tenantAId = UUID.randomUUID();
        UUID tenantBId = UUID.randomUUID();

        //save first one
        {
            Organization organization = new Organization(tenantAId);
            assertNull(organization.getId());

            organization.setName("Fred");

            crudService.create(organization);
            assertNotNull(organization.getId());

            jpaRule.changeTransaction();
        }

        //save same name in different tenant
        {
            Organization organization = new Organization(tenantBId);
            assertNull(organization.getId());

            organization.setName("Fred");

            crudService.create(organization);
            jpaRule.changeTransaction();
        }
    }

    @Test
    public void testConstraintViolations() {

        Organization organization = new Organization();

        expectedException.expect(new ConstraintViolationMatcher("Organization Name may not be null", "Tenant ID may not be null"));
        crudService.create(organization);
    }

    @Test
    public void testCreateQuery() {

        List<QueryParameters.ColumnSort> columnSortList = new ArrayList<>();
        columnSortList.add(new QueryParameters.ColumnSort("name", SortDirection.DESC));
        QueryParameters queryParameters = new QueryParameters(0, QueryParameters.DEFAULT_LIMIT, columnSortList);
        QueryParametersHelper.convertPropertyStringsToQueryPaths(queryParameters, QOrganization.organization);


        crudService.createQuery(queryParameters, QOrganization.organization);

    }

    @Test
    public void testCreateQueryUnknownSortColumnName() {

        List<QueryParameters.ColumnSort> columnSortList = new ArrayList<>();
        columnSortList.add(new QueryParameters.ColumnSort("bad", SortDirection.DESC));
        QueryParameters queryParameters = new QueryParameters(0, QueryParameters.DEFAULT_LIMIT, columnSortList);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unknown Property name");
        QueryParametersHelper.convertPropertyStringsToQueryPaths(queryParameters, QOrganization.organization);
    }

    @Test
    public void testCreateQueryNonComparableSortColumnName() {

        List<QueryParameters.ColumnSort> columnSortList = new ArrayList<>();
        columnSortList.add(new QueryParameters.ColumnSort("businessRoles", SortDirection.DESC));
        QueryParameters queryParameters = new QueryParameters(0, QueryParameters.DEFAULT_LIMIT, columnSortList);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("isn't valid for sorting (doesn't implement Comparable)");
        QueryParametersHelper.convertPropertyStringsToQueryPaths(queryParameters, QOrganization.organization);
    }

    @Test
    public void testCreateQueryNonAccessibleSortColumnName() {

        List<QueryParameters.ColumnSort> columnSortList = new ArrayList<>();
        columnSortList.add(new QueryParameters.ColumnSort("serialVersionUID", SortDirection.DESC));
        QueryParameters queryParameters = new QueryParameters(0, QueryParameters.DEFAULT_LIMIT, columnSortList);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("isn't accessible");
        QueryParametersHelper.convertPropertyStringsToQueryPaths(queryParameters, QOrganization.organization);
    }

    private class OrganizationBusinessRoleMatcher extends TypeSafeMatcher<OrganizationBusinessRole> {

        private final boolean active;

        private OrganizationBusinessRoleMatcher(boolean active) {
            this.active = active;
        }

        @Override
        protected boolean matchesSafely(OrganizationBusinessRole organizationBusinessRole) {
            return organizationBusinessRole.isActive() == active;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("expects ").appendValue(active);

        }
    }
}
