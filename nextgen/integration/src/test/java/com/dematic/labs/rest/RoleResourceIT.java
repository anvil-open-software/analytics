package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.business.SecurityManagerIT;
import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
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

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RoleResourceIT extends SecuredEndpointFixture {

    private static String uuid, roleUuid;

    public RoleResourceIT() throws MalformedURLException {
    }

    @BeforeClass
    public static void before() throws MalformedURLException {

        SignatureToken token = getToken(tenant, username, password);

        //create tenant
        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant").toExternalForm()));

            TenantDto tenantDto = new TenantDto();
            tenantDto.setName(SecurityManagerIT.NEW_TENANT);

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.POST, MediaType.APPLICATION_JSON
            ).post(Entity.entity(tenantDto, MediaType.APPLICATION_JSON_TYPE));

            assertNotNull(response);
            assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

            String location = response.getLocation().toString();
            String[] locationElements = location.split("/");
            uuid = locationElements[locationElements.length-1];
        }

        //create tenant admin
        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenantAdminUser").toExternalForm()));

            UserDto userDto = new UserDto();
            TenantDto tenantDto = new TenantDto();
            tenantDto.setName(SecurityManagerIT.NEW_TENANT);
            userDto.setTenantDto(tenantDto);
            userDto.setLoginName(SecurityManagerIT.TENANT_ADMIN_USERNAME);
            userDto.setPassword(SecurityManagerIT.TENANT_ADMIN_PASSWORD);
            assertNull(userDto.getId());

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.POST, MediaType.APPLICATION_JSON
            ).post(Entity.entity(userDto, MediaType.APPLICATION_JSON_TYPE));

            assertNotNull(response);
            assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

        }
    }

    @Test
    public void test01Create() throws MalformedURLException {

        SignatureToken token = getToken(SecurityManagerIT.NEW_TENANT,
                SecurityManagerIT.TENANT_ADMIN_USERNAME, SecurityManagerIT.TENANT_ADMIN_PASSWORD);

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

        assertNotNull(response);
        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

        RoleDto fromServer = response.readEntity(RoleDto.class);

        assertNotNull(fromServer);
        assertNotNull(fromServer.getId());
        roleUuid = fromServer.getId();
        assertEquals(SecurityManagerIT.CUSTOM_TENANT_ROLE, fromServer.getName());

    }

    @Test
    public void test02GetList() throws Exception {

        SignatureToken token = getToken(SecurityManagerIT.NEW_TENANT,
                SecurityManagerIT.TENANT_ADMIN_USERNAME, SecurityManagerIT.TENANT_ADMIN_PASSWORD);

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/role").toExternalForm()));

        RoleDto[] list = signRequest(token, target
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
        ).get(RoleDto[].class);

        assertNotNull(list);
        assertEquals(ApplicationRole.getTenantRoles().size() + 1, list.length);
    }

    @Test
    public void test03Delete() throws Exception {

        SignatureToken token = getToken(SecurityManagerIT.NEW_TENANT,
                SecurityManagerIT.TENANT_ADMIN_USERNAME, SecurityManagerIT.TENANT_ADMIN_PASSWORD);

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

        SignatureToken token = getToken(tenant, username, password);

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant/" + uuid).toExternalForm()));

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(SecurityManagerIT.NEW_TENANT);

        signRequest(token, target.request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.DELETE, null
        ).delete();

    }

}