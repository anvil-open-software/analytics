package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.business.dto.CollectionDto;
import com.dematic.labs.business.dto.OrganizationBusinessRoleDto;
import com.dematic.labs.business.dto.OrganizationDto;
import com.dematic.labs.business.dto.UserDto;
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
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.getBase;
import static com.dematic.labs.rest.SecuredEndpointHelper.getToken;
import static com.dematic.labs.rest.SecuredEndpointHelper.signRequest;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrganizationResourceIT {

    private static String tenantUuid, organizationUuid;

   @BeforeClass
    public static void before() throws MalformedURLException {

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
    public void test010GetBusinessRoles() throws Exception {
        SignatureToken token = getToken(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/businessRole").toExternalForm()));

        String[] list = signRequest(token, target
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
        ).get(String[].class);

        List<Matcher<? super String>> businessRoleNameMatcherList = Arrays.asList(BusinessRole.values()).stream()
                .map(BusinessRole::name).map(IsEqual::<String>equalTo).collect(Collectors.toList());
        assertThat(list, arrayContainingInAnyOrder(businessRoleNameMatcherList));
    }

    @Test
    public void test020Post() throws MalformedURLException {

        String organizationName = "ACME";

        SignatureToken token = getToken(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);
        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/organization").toExternalForm()));

            OrganizationDto organizationDto = new OrganizationDto();
            organizationDto.setName(organizationName);

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.POST, MediaType.APPLICATION_JSON
                    ).post(Entity.entity(organizationDto, MediaType.APPLICATION_JSON));

            assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
            OrganizationDto fromServer = response.readEntity(OrganizationDto.class);

            assertThat(response, new CreatedResponseMatcher<>(fromServer, new IdentifiableDtoHrefMatcher<>()));

            assertEquals(organizationName, fromServer.getName());
            organizationUuid = fromServer.getId();
        }

    }

    @Test
    public void test030GetList() throws Exception {
        SignatureToken token = getToken(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/organization").toExternalForm()));

        CollectionDto<OrganizationDto> collectionDto = signRequest(token, target
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
                ).get(new GenericType<CollectionDto<OrganizationDto>>() {
        });

        assertThat(collectionDto.getItems(), iterableWithSize(1));
        assertThat(collectionDto.getItems(), everyItem(new IdentifiableDtoHrefMatcher<>()));
    }

    @Test
    public void test040GrantBusinessRole() throws MalformedURLException {

        SignatureToken token = getToken(TENANT_A, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

        OrganizationDto organizationDto = OrganizationHelper.createOrganization(token, organizationUuid);

        //grant business role
        organizationDto = OrganizationHelper.grantBusinessRoles(token, organizationDto,
                new OrganizationBusinessRoleDto(BusinessRole.CUSTOMER, true));

        //grant/revoke business role (update)
        OrganizationHelper.grantBusinessRoles(token, organizationDto,
                new OrganizationBusinessRoleDto(BusinessRole.SUPPLIER, false));

    }

    @AfterClass
    public static void after() throws MalformedURLException {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        IdentityManagementHelper.deleteTenant(token, tenantUuid);

    }

}