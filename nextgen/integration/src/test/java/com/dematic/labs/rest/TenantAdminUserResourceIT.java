package com.dematic.labs.rest;

import com.dematic.labs.business.SecurityFixture;
import com.dematic.labs.business.dto.CollectionDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.business.matchers.UserDtoMatcher;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.dto.RestError;
import com.dematic.labs.rest.matchers.UserDtoHrefMatcher;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.StringStartsWith;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.*;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TenantAdminUserResourceIT {

    private static SignatureToken token;
    private static List<String> uuidList = new ArrayList<>();

    public TenantAdminUserResourceIT() {
    }

    @BeforeClass
    public static void before() {

        token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        uuidList.add(IdentityManagementHelper.createTenant(token, TENANT_A).getId());
        uuidList.add(IdentityManagementHelper.createTenant(token, TENANT_B).getId());
    }

    @Test
    public void test01Create() {

        IdentityManagementHelper.createTenantAdmin(token, TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

    }

    @Test
    public void test02CreateDuplicate() {

        {
            Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                    .path("resources/tenantAdminUser")
                    .request(MediaType.APPLICATION_JSON);

            UserDto userDto = new UserDto();
            TenantDto tenantDto = new TenantDto();
            tenantDto.setName(TENANT_A);
            userDto.setTenantDto(tenantDto);
            userDto.setLoginName(TENANT_A_ADMIN_USERNAME);
            userDto.setPassword(SecurityFixture.TENANT_A_ADMIN_PASSWORD);
            assertNull(userDto.getId());

            Response response = request
                    .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                    .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                            signRequest(request, token, HttpMethod.POST, MediaType.APPLICATION_JSON))
                    .post(Entity.entity(userDto, MediaType.APPLICATION_JSON_TYPE));

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
        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenantAdminUser")
                .request(MediaType.APPLICATION_JSON);

        CollectionDto<UserDto> collectionDto = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get(new GenericType<CollectionDto<UserDto>>() {});

        assertThat(collectionDto.getItems(), iterableWithSize(1));
        assertThat(collectionDto.getItems(), everyItem(new UserDtoHrefMatcher()));
    }

    @Test
    public void test04GetListWithSort() throws Exception {

        IdentityManagementHelper.createTenantAdmin(token, TENANT_B, TENANT_B_ADMIN_USERNAME, TENANT_B_ADMIN_PASSWORD);
        IdentityManagementHelper.createTenantAdmin(token, TENANT_B, "second"+TENANT_B_ADMIN_USERNAME, TENANT_B_ADMIN_PASSWORD);
        IdentityManagementHelper.createTenantAdmin(token, TENANT_B, "last"+TENANT_B_ADMIN_USERNAME, TENANT_B_ADMIN_PASSWORD);

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenantAdminUser")
                .queryParam("orderBy", "tenantDto.name DESC,loginName".replace(" ", "%20"))
                .request(MediaType.APPLICATION_JSON);

        CollectionDto<UserDto> collectionDto = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get(new GenericType<CollectionDto<UserDto>>() {});

        assertThat(collectionDto.getItems(), iterableWithSize(4));
        assertThat(collectionDto.getItems(), everyItem(new UserDtoHrefMatcher()));
        assertThat(collectionDto.getItems(), IsIterableContainingInOrder.contains(
                new UserDtoMatcher(TENANT_B_ADMIN_USERNAME, TENANT_B),
                new UserDtoMatcher("last"+TENANT_B_ADMIN_USERNAME, TENANT_B),
                new UserDtoMatcher("second"+TENANT_B_ADMIN_USERNAME, TENANT_B),
                new UserDtoMatcher(TENANT_A_ADMIN_USERNAME, TENANT_A)));
    }

    @AfterClass
    public static void after() {

        uuidList.forEach(p -> IdentityManagementHelper.deleteTenant(token, p));

    }

}