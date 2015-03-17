package com.dematic.labs.rest;

import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.matchers.UserDtoHrefMatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.List;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.*;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UserResourceIT {

    private static String uuid;

    public UserResourceIT() throws MalformedURLException {
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

        IdentityManagementHelper.createTenantUser(token, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

    }

    @Test
    public void test03GetList() throws Exception {

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/user").toExternalForm()));

        List<UserDto> list = signRequest(token, target
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
        ).get(new GenericType<List<UserDto>>() {});

        assertThat(list, iterableWithSize(2));
        assertThat(list, everyItem(new UserDtoHrefMatcher()));
    }

    @AfterClass
    public static void after() throws MalformedURLException {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        IdentityManagementHelper.deleteTenant(token, uuid);

    }

}