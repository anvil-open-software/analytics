package com.dematic.labs.persistence.entities;

import com.dematic.labs.persistence.CrudService;
import com.dematic.labs.persistence.EmbeddedH2;
import com.dematic.labs.persistence.JpaRule;
import org.h2.jdbc.JdbcSQLException;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

import static junit.framework.TestCase.*;

public class OrganizationTest {

    private CrudService crudService;

    @Rule
    public JpaRule jpaRule = new JpaRule(new EmbeddedH2(), "NewPersistenceUnit");

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        crudService = new CrudService(jpaRule.getEntityManager());
    }

    @Test
    public void testSave() {

        UUID tenantId = UUID.randomUUID();

        Organization organization = new Organization(tenantId);
        assertNull(organization.getId());

        organization.setName("Fred");

        crudService.create(organization);
        assertNotNull(organization.getId());

        jpaRule.changeTransaction();

        Organization organizationFromDb = crudService.findExisting(organization.getId(), Organization.class);
        assertEquals(organization.getId(), organizationFromDb.getId());
        assertEquals("Fred", organizationFromDb.getName());
    }

    @Test
    public void testUniqueWithinTenant() {

        UUID tenantId = UUID.randomUUID();

        //save first one
        {
            Organization organization = new Organization(tenantId);
            assertNull(organization.getId());

            organization.setName("Fred");

            crudService.create(organization);
            assertNotNull(organization.getId());

            jpaRule.changeTransaction();
        }

        //attempt saving duplicate
        {
            Organization organization = new Organization(tenantId);
            assertNull(organization.getId());

            organization.setName("Fred");

            exception.expectCause(new HibernateWrappedCauseMatcher(ConstraintViolationException.class,
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

    private static class HibernateWrappedCauseMatcher extends TypeSafeMatcher<Throwable> {

        private final Class<? extends Throwable> type, cause;
        private final String expectedMessage;

        public HibernateWrappedCauseMatcher(Class<? extends Throwable> type, Class<? extends Throwable> cause, String expectedMessage) {
            this.type = type;
            this.cause = cause;
            this.expectedMessage = expectedMessage;
        }

        @Override
        protected boolean matchesSafely(Throwable item) {
            return item.getClass().isAssignableFrom(type)
                    && item.getCause().getClass().isAssignableFrom(cause)
                    && item.getCause().getMessage().contains(expectedMessage);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("expects type ")
                    .appendValue(type)
                    .appendText(" expects cause ")
                    .appendValue(cause)
                    .appendText(" and a message ")
                    .appendValue(expectedMessage);
        }
    }
}
