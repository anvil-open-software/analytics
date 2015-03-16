package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.business.SecurityManagerIT;
import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.matchers.CreatedResponseMatcher;
import com.dematic.labs.rest.matchers.IdentifiableDtoHrefMatcher;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.List;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.getBase;
import static com.dematic.labs.rest.SecuredEndpointHelper.getToken;
import static com.dematic.labs.rest.SecuredEndpointHelper.signRequest;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RoleResourceIT {

    private static String uuid, roleUuid;

    public RoleResourceIT() throws MalformedURLException {
    }

    @BeforeClass
    public static void before() throws MalformedURLException {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        uuid = IdentityManagementHelper.createTenant(token, TENANT_A).getId();

        IdentityManagementHelper.createTenantAdmin(token, TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);
    }

    @Test
    public void test01Create() throws MalformedURLException {

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/role").toExternalForm()));

        RoleDto roleDto = new RoleDto();
        roleDto.setName(SecurityManagerIT.CUSTOM_TENANT_ROLE);
        assertNull(roleDto.getId());

        Response response = signRequest(token, target.request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.POST, MediaType.APPLICATION_JSON
        ).post(Entity.entity(roleDto, MediaType.APPLICATION_JSON_TYPE));

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        RoleDto fromServer = response.readEntity(RoleDto.class);

        assertThat(response, new CreatedResponseMatcher<>(fromServer, new IdentifiableDtoHrefMatcher<>()));

        assertEquals(SecurityManagerIT.CUSTOM_TENANT_ROLE, fromServer.getName());

        roleUuid = fromServer.getId();

    }

    @Test
    public void test02GetList() throws Exception {

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        List<RoleDto> list = IdentityManagementHelper.getRoles(token);

        assertThat(list, iterableWithSize(ApplicationRole.getTenantRoles().size() + 1));
    }

    @Test
    public void test03Delete() throws Exception {

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/role/" + roleUuid).toExternalForm()));

        Response response = signRequest(token, target
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.DELETE, null
        ).delete();

        assertNotNull(response);
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    }

    @AfterClass
    public static void after() throws MalformedURLException {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        IdentityManagementHelper.deleteTenant(token, uuid);

    }

}