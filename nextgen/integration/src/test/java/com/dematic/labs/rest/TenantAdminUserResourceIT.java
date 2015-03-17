package com.dematic.labs.rest;

import com.dematic.labs.business.SecurityFixture;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.dto.RestError;
import com.dematic.labs.rest.matchers.UserDtoHrefMatcher;
import org.hamcrest.core.StringStartsWith;
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
import java.util.List;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.getBase;
import static com.dematic.labs.rest.SecuredEndpointHelper.getToken;
import static com.dematic.labs.rest.SecuredEndpointHelper.signRequest;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TenantAdminUserResourceIT {

    private static SignatureToken token;
    private static String uuid;

    public TenantAdminUserResourceIT() throws MalformedURLException {
    }

    @BeforeClass
    public static void before() throws MalformedURLException {

        token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        uuid = IdentityManagementHelper.createTenant(token, TENANT_A).getId();
    }

    @Test
    public void test01Create() throws MalformedURLException {

        IdentityManagementHelper.createTenantAdmin(token, TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

    }

    @Test
    public void test02CreateDuplicate() throws MalformedURLException {

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenantAdminUser").toExternalForm()));

            UserDto userDto = new UserDto();
            TenantDto tenantDto = new TenantDto();
            tenantDto.setName(TENANT_A);
            userDto.setTenantDto(tenantDto);
            userDto.setLoginName(TENANT_A_ADMIN_USERNAME);
            userDto.setPassword(SecurityFixture.TENANT_A_ADMIN_PASSWORD);
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

        List<UserDto> list = signRequest(token, target
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
        ).get(new GenericType<List<UserDto>>() {});

        assertThat(list, iterableWithSize(1));
        assertThat(list, everyItem(new UserDtoHrefMatcher()));
    }

    @AfterClass
    public static void after() throws MalformedURLException {

        IdentityManagementHelper.deleteTenant(token, uuid);

    }

}