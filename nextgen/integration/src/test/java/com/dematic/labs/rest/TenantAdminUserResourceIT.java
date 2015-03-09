package com.dematic.labs.rest;

import com.dematic.labs.business.SecurityManagerIT;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.dto.RestError;
import org.hamcrest.core.StringStartsWith;
import org.junit.*;
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

import static com.dematic.labs.picketlink.SecurityInitializer.INSTANCE_ADMIN_PASSWORD;
import static com.dematic.labs.picketlink.SecurityInitializer.INSTANCE_ADMIN_USERNAME;
import static com.dematic.labs.picketlink.SecurityInitializer.INSTANCE_TENANT_NAME;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TenantAdminUserResourceIT extends SecuredEndpointFixture {

    private static SignatureToken token;
    private static String uuid;

    public TenantAdminUserResourceIT() throws MalformedURLException {
    }

    @BeforeClass
    public static void before() throws MalformedURLException {

        token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

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

    @Test
    public void test01Create() throws MalformedURLException {

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

            String location = response.getLocation().toString();
            String[] locationElements = location.split("/");
            String uuid = locationElements[locationElements.length-1];

            UserDto fromServer = response.readEntity(UserDto.class);

            assertNotNull(fromServer);
            assertNotNull(fromServer.getId());
            assertFalse(fromServer.getId().isEmpty());
            assertEquals(uuid, fromServer.getId());
        }

    }

    @Test
    public void test02CreateDuplicate() throws MalformedURLException {

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
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());

            RestError error = response.readEntity(RestError.class);
            assertNotNull(error);
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), error.getHttpStatusCode());
            assertThat(error.getMessage(), new StringStartsWith("IdentityType [class org.picketlink.idm.model.basic.User] already exists with the given identifier"));
        }

    }

    @Test
    public void test03GetList() throws Exception {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenantAdminUser").toExternalForm()));

        UserDto[] list = signRequest(token, target
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
        ).get(UserDto[].class);

        assertNotNull(list);
        assertTrue(list.length > 0);
    }

    @AfterClass
    public static void after() throws MalformedURLException {

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