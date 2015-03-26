package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.business.dto.CollectionDto;
import com.dematic.labs.business.dto.OrganizationBusinessRoleDto;
import com.dematic.labs.business.dto.OrganizationDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.business.matchers.NamedDtoMatcher;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.persistence.entities.BusinessRole;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.matchers.IdentifiableDtoUriMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.IsEqual;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrganizationResourceIT {

    private static String tenantUuid, organizationUuid;

   @BeforeClass
    public static void before() {

        {
            SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

            tenantUuid = IdentityManagementHelper.createTenant(token, TENANT_A).getId();

            IdentityManagementHelper.createTenantAdmin(token, TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);
        }

        {
            SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

            UserDto tenantUserDto = IdentityManagementHelper.createTenantUser(token, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

            IdentityManagementHelper.grantRoles(token, tenantUserDto,
                    ApplicationRole.VIEW_ORGANIZATIONS,
                    ApplicationRole.CREATE_ORGANIZATIONS,
                    ApplicationRole.ADMINISTER_ORGANIZATION_BUSINESS_ROLES);
        }
    }

    @Test
    public void test010GetBusinessRoles() {

        SignatureToken token = getToken(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/businessRole")
                .request(MediaType.APPLICATION_JSON);

        List<String> list = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get(new GenericType<List<String>>() {});

        List<Matcher<? super String>> businessRoleNameMatcherList = Arrays.asList(BusinessRole.values()).stream()
                .map(BusinessRole::name).map(IsEqual::<String>equalTo).collect(Collectors.toList());
        assertThat(list, contains(businessRoleNameMatcherList));
    }

    @Test
    public void test020Post() {

        SignatureToken token = getToken(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);
        organizationUuid = OrganizationHelper.createOrganization(token, "ACME").getId();

    }

    @Test
    public void test030GetList() {

        SignatureToken token = getToken(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

        OrganizationHelper.createOrganization(token, "ZEBRA");
        OrganizationHelper.createOrganization(token, "SPAM");
        OrganizationHelper.createOrganization(token, "NAVIS");

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/organization")
                .queryParam("limit", "3")
                .queryParam("orderBy", "name DESC".replace(" ", "%20"))
                .request(MediaType.APPLICATION_JSON);

        CollectionDto<OrganizationDto> collectionDto = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get(new GenericType<CollectionDto<OrganizationDto>>() {});

        assertThat(collectionDto.getItems(), iterableWithSize(collectionDto.getQueryParameters().getLimit()));
        assertThat(collectionDto.getItems(), everyItem(new IdentifiableDtoUriMatcher<>()));
        assertThat(collectionDto.getItems(), new IsIterableContainingInOrder<>(Arrays.asList(
                new NamedDtoMatcher<>("ZEBRA"),
                new NamedDtoMatcher<>("SPAM"),
                new NamedDtoMatcher<>("NAVIS"))));
    }

    @Test
    public void test040GrantBusinessRole() {

        SignatureToken token = getToken(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

        OrganizationDto organizationDto = OrganizationHelper.getOrganization(token, organizationUuid);

        //grant business role
        organizationDto = OrganizationHelper.grantBusinessRoles(token, organizationDto,
                new OrganizationBusinessRoleDto(BusinessRole.CUSTOMER, true));

        //grant/revoke business role (update)
        OrganizationHelper.grantBusinessRoles(token, organizationDto,
                new OrganizationBusinessRoleDto(BusinessRole.SUPPLIER, false));

    }

    @AfterClass
    public static void after() {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        IdentityManagementHelper.deleteTenant(token, tenantUuid);

    }

}