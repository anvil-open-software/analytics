package com.dematic.labs.business;

import com.dematic.labs.business.dto.OrganizationDto;
import com.dematic.labs.persistence.CrudService;
import com.dematic.labs.persistence.entities.*;
import org.apache.deltaspike.security.api.authorization.AccessDeniedException;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import javax.inject.Inject;
import java.util.List;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static org.junit.Assert.*;


@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrganizationManagerIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Inject
    public SecurityFixture securityFixture;

    @Inject
    OrganizationManager organizationManager;

    @Deployment
    public static WebArchive createDeployment() {

        return SecurityFixture.createDeployment("org.postgresql:postgresql",
                        PostgresDataSourceDefinitionHolder.class,
                        TwoTenantSecurityInitializer.class)
                    .addClasses(Organization.class,
                            OwnedAssetEntity.class,
                            IdentifiableEntity.class,
                            OrganizationDto.class,
                            QOrganization.class,
                            QOwnedAssetEntity.class,
                            QIdentifiableEntity.class,
                            OrganizationManager.class,
                            CrudService.class);
    }

    @Test
    public void test010SaveWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.create(new OrganizationDto());
    }

    @Test
    public void test020SaveWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(TENANT_A, SecurityFixture.TENANT_A_USER_USERNAME,
                SecurityFixture.TENANT_A_USER_PASSWORD);

        OrganizationDto organizationDto = new OrganizationDto();
        organizationDto.setName("ACME");
        assertNull(organizationDto.getId());

        OrganizationDto persistedPrincipal = organizationManager.create(organizationDto);
        assertNotNull(persistedPrincipal.getId());
        assertEquals(organizationDto.getName(), persistedPrincipal.getName());
    }

    @Test
    public void test025SaveWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(TENANT_B, SecurityFixture.TENANT_B_USER_USERNAME,
                SecurityFixture.TENANT_B_USER_PASSWORD);

        OrganizationDto organizationDto = new OrganizationDto();
        organizationDto.setName("ACME");
        assertNull(organizationDto.getId());

        OrganizationDto persistedPrincipal = organizationManager.create(organizationDto);
        assertNotNull(persistedPrincipal.getId());
        assertEquals(organizationDto.getName(), persistedPrincipal.getName());
    }

    @Test
    public void test030SaveWithoutAuthorization() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.create(new OrganizationDto());
    }

    @Test
    public void test040ListWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.getOrganizations();
    }

    @Test
    public void test050ListWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(TENANT_A, SecurityFixture.TENANT_A_USER_USERNAME,
                SecurityFixture.TENANT_A_USER_PASSWORD);

       List<OrganizationDto> organizationDtos = organizationManager.getOrganizations();
       assertEquals(1, organizationDtos.size());

    }

    @Test
    public void test060SaveWithoutAuthorization() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.getOrganizations();
    }

}