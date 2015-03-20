package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.business.dto.*;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.persistence.entities.BusinessRole;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.matchers.CreatedResponseMatcher;
import com.dematic.labs.rest.matchers.IdentifiableDtoHrefMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsEqual;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.*;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
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

        String organizationName = "ACME";

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/organization")
                .request(MediaType.APPLICATION_JSON);

        OrganizationDto organizationDto = new OrganizationDto();
        organizationDto.setName(organizationName);

        SignatureToken token = getToken(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.POST, MediaType.APPLICATION_JSON))
                .post(Entity.entity(organizationDto, MediaType.APPLICATION_JSON));

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        OrganizationDto fromServer = response.readEntity(OrganizationDto.class);

        assertThat(response, new CreatedResponseMatcher<>(fromServer, new IdentifiableDtoHrefMatcher<>()));

        assertEquals(organizationName, fromServer.getName());
        organizationUuid = fromServer.getId();

    }

    @Test
    public void test030GetList() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/organization")
                .request(MediaType.APPLICATION_JSON);

        SignatureToken token = getToken(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

        CollectionDto<OrganizationDto> collectionDto = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get(new GenericType<CollectionDto<OrganizationDto>>() {});

        assertThat(collectionDto.getItems(), iterableWithSize(1));
        assertThat(collectionDto.getItems(), everyItem(new IdentifiableDtoHrefMatcher<>()));
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