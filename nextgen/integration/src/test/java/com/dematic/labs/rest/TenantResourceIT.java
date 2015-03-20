package com.dematic.labs.rest;

import com.dematic.labs.business.dto.CollectionDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.dto.RestError;
import com.dematic.labs.rest.matchers.IdentifiableDtoHrefMatcher;
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

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TenantResourceIT {

    private static String uuid;

    public TenantResourceIT() {
    }

    @BeforeClass
    public static void before() {
        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        uuid = IdentityManagementHelper.createTenant(token, TENANT_A).getId();

        IdentityManagementHelper.createTenantAdmin(token, TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);
    }

    @Test
    public void test01GetList() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .request(MediaType.APPLICATION_JSON);

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        CollectionDto<TenantDto> collectionDto = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get(new GenericType<CollectionDto<TenantDto>>() {});

        assertThat(collectionDto.getItems(), not(empty()));
        assertThat(collectionDto.getItems(), everyItem(new IdentifiableDtoHrefMatcher<>()));
    }

    @Test
    public void test01GetListWithPaginationOffsetOvershoot() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .queryParam("offset", "10")
                .request(MediaType.APPLICATION_JSON);

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get();

        assertNotNull(response);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());

        RestError error = response.readEntity(RestError.class);
        assertNotNull(error);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), error.getHttpStatusCode());
        assertEquals(Response.Status.BAD_REQUEST.getReasonPhrase(), error.getHttpStatus());
        assertEquals("Offset [10] exceeds size of collection [3]", error.getMessage());
    }

    /*
     Need one test of failing authorization to cover 1) Picketlink SecurityInterceptor is wired correctly and
     2) that ExceptionMapping is wired correctly
     */
    @Test
    public void test02GetListWithoutAuthorization() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .request(MediaType.APPLICATION_JSON);

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get();

        assertNotNull(response);
        assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());

        RestError error = response.readEntity(RestError.class);
        assertNotNull(error);
        assertEquals(Response.Status.FORBIDDEN.getStatusCode(), error.getHttpStatusCode());
    }

    @Test
    public void test03GetListInvalidPagination() {

       Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                        .path("resources/tenant")
                        .queryParam("limit", -1)
                        .queryParam("offset", -1)
                        .request(MediaType.APPLICATION_JSON);

       SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

       Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                 .get();

        assertNotNull(response);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());

        RestError error = response.readEntity(RestError.class);
        assertNotNull(error);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), error.getHttpStatusCode());
        assertThat(error.getConstraintViolationMessages(), containsInAnyOrder("Pagination offset must be positive",
                "Pagination limit must be positive"));
    }

    @Test
    public void test04CreateDuplicate() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .request(MediaType.APPLICATION_JSON_TYPE);

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(TENANT_A);

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.POST, MediaType.APPLICATION_JSON))
                .post(Entity.entity(tenantDto, MediaType.APPLICATION_JSON_TYPE));

        assertNotNull(response);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());

        RestError error = response.readEntity(RestError.class);
        assertNotNull(error);
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), error.getHttpStatusCode());
        assertEquals("Duplicate Tenant Name", error.getMessage());

    }

    @AfterClass
    public static void after() {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        IdentityManagementHelper.deleteTenant(token, uuid);

    }

}