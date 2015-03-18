package com.dematic.labs.rest;

import com.dematic.labs.business.dto.CollectionDto;
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
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.time.Instant;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.*;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UserResourceIT {

    private static String uuid;

    public UserResourceIT() {
    }

    @BeforeClass
    public static void before() {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        uuid = IdentityManagementHelper.createTenant(token, TENANT_A).getId();

        IdentityManagementHelper.createTenantAdmin(token, TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);
    }

    @Test
    public void test01Create() {

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        IdentityManagementHelper.createTenantUser(token, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

    }

    @Test
    public void test03GetList() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/user")
                .request(MediaType.APPLICATION_JSON);

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);


        CollectionDto<UserDto> collectionDto = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get(new GenericType<CollectionDto<UserDto>>() {});

        assertThat(collectionDto.getItems(), iterableWithSize(2));
        assertThat(collectionDto.getItems(), everyItem(new UserDtoHrefMatcher()));
    }

    @AfterClass
    public static void after() {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        IdentityManagementHelper.deleteTenant(token, uuid);

    }

}