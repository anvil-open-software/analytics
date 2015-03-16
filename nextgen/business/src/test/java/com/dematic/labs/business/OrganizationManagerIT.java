package com.dematic.labs.business;

import com.dematic.labs.business.dto.OrganizationBusinessRoleDto;
import com.dematic.labs.business.dto.OrganizationDto;
import com.dematic.labs.business.matchers.OrganizationBusinessRoleDtoMatcher;
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
import java.util.UUID;

import static com.dematic.labs.business.SecurityFixture.TENANT_A;
import static com.dematic.labs.business.SecurityFixture.TENANT_B;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.*;


@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrganizationManagerIT {

    private static UUID organizationId;

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
                            OrganizationBusinessRole.class,
                            BusinessRole.class,
                            OrganizationDto.class,
                            OrganizationBusinessRoleDto.class,
                            OrganizationBusinessRoleDtoMatcher.class,
                            QOrganization.class,
                            QOwnedAssetEntity.class,
                            QIdentifiableEntity.class,
                            QOrganizationBusinessRole.class,
                            OrganizationManager.class,
                            CrudService.class);
    }

    @Test
    public void test010CreateWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.create(new OrganizationDto());
    }

    @Test
    public void test020CreateWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(TENANT_A, SecurityFixture.TENANT_A_USER_USERNAME,
                SecurityFixture.TENANT_A_USER_PASSWORD);

        OrganizationDto organizationDto = new OrganizationDto();
        organizationDto.setName("ACME");
        assertNull(organizationDto.getId());

        OrganizationDto persistedOrganization = organizationManager.create(organizationDto);
        assertNotNull(persistedOrganization.getId());
        organizationId = UUID.fromString(persistedOrganization.getId());
        assertEquals(organizationDto.getName(), persistedOrganization.getName());
    }

    @Test
    public void test030CreateWithoutAuthorization() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.create(new OrganizationDto());
    }

    /*
     Tests that TenantB can have an identically named organization as another tenant (Tenant A)
     */
    @Test
    public void test040TenantOrgNameUniqueness() throws Exception {

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
    public void test050GetWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.getOrganization(organizationId);
    }

    @Test
    public void test060GetWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(TENANT_A, SecurityFixture.TENANT_A_USER_USERNAME,
                SecurityFixture.TENANT_A_USER_PASSWORD);

        OrganizationDto organizationDto = organizationManager.getOrganization(organizationId);
        assertNotNull(organizationDto);
        assertEquals(organizationId.toString(), organizationDto.getId());

    }

    @Test
    public void test070GetWithoutAuthorization() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.getOrganization(organizationId);
    }

    @Test
    public void test080ListWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.getOrganizations();
    }

    @Test
    public void test090ListWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(TENANT_A, SecurityFixture.TENANT_A_USER_USERNAME,
                SecurityFixture.TENANT_A_USER_PASSWORD);

       List<OrganizationDto> organizationDtoList = organizationManager.getOrganizations();
       assertEquals(1, organizationDtoList.size());

    }

    @Test
    public void test100ListWithoutAuthorization() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.getOrganizations();
    }

    @Test
    public void test110GrantBusinessRoleWithoutAuthentication() throws Exception {

        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.grantRevokeBusinessRole(new OrganizationDto());
    }

    @Test
    public void test120GrantBusinessRoleWithAuthenticationAndAuthorization() throws Exception {

        securityFixture.login(TENANT_A, SecurityFixture.TENANT_A_USER_USERNAME,
                SecurityFixture.TENANT_A_USER_PASSWORD);

        //grant business roles
        OrganizationDto organizationDto;
        {
            organizationDto = organizationManager.getOrganization(organizationId);
            organizationDto.getBusinessRoles().add(new OrganizationBusinessRoleDto(BusinessRole.CUSTOMER, true));
            organizationDto.getBusinessRoles().add(new OrganizationBusinessRoleDto(BusinessRole.CARRIER, false));

            organizationManager.grantRevokeBusinessRole(organizationDto);

            OrganizationDto persistedOrganizationDto = organizationManager.getOrganization(UUID.fromString(organizationDto.getId()));

            assertEquals(2, persistedOrganizationDto.getBusinessRoles().size());
            assertThat(persistedOrganizationDto.getBusinessRoles(), hasItems(
                    new OrganizationBusinessRoleDtoMatcher(BusinessRole.CUSTOMER, true),
                    new OrganizationBusinessRoleDtoMatcher(BusinessRole.CARRIER, false)));
        }

        //grant/revoke (update) business roles
        {
            organizationDto.getBusinessRoles().clear();
            organizationDto.getBusinessRoles().add(new OrganizationBusinessRoleDto(BusinessRole.SUPPLIER, true));
            organizationDto.getBusinessRoles().add(new OrganizationBusinessRoleDto(BusinessRole.OPERATOR, false));

            organizationManager.grantRevokeBusinessRole(organizationDto);

            OrganizationDto persistedOrganizationDto = organizationManager.getOrganization(UUID.fromString(organizationDto.getId()));

            assertEquals(2, persistedOrganizationDto.getBusinessRoles().size());
            assertThat(persistedOrganizationDto.getBusinessRoles(), hasItems(
                    new OrganizationBusinessRoleDtoMatcher(BusinessRole.SUPPLIER, true),
                    new OrganizationBusinessRoleDtoMatcher(BusinessRole.OPERATOR, false)));
        }
    }

    @Test
    public void test130GrantBusinessRoleWithoutAuthorization() throws Exception {

        securityFixture.login(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        exception.expectMessage(AccessDeniedException.class.getName());
        organizationManager.grantRevokeBusinessRole(new OrganizationDto());
    }

}