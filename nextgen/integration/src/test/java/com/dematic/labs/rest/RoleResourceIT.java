package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.business.SecurityManagerIT;
import com.dematic.labs.business.dto.CollectionDto;
import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.matchers.CreatedResponseMatcher;
import com.dematic.labs.rest.matchers.IdentifiableDtoUriMatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Instant;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.*;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RoleResourceIT {

    private static String uuid, roleUuid;

    public RoleResourceIT() {
    }

    @BeforeClass
    public static void before() {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        uuid = IdentityManagementHelper.createTenant(token, TENANT_A).getId();

        IdentityManagementHelper.createTenantAdmin(token, TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);
    }

    @Test
    public void test01Create() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/role")
                .request(MediaType.APPLICATION_JSON_TYPE);

        RoleDto roleDto = new RoleDto();
        roleDto.setName(SecurityManagerIT.CUSTOM_TENANT_ROLE);
        assertNull(roleDto.getId());

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.POST, MediaType.APPLICATION_JSON))
                .post(Entity.entity(roleDto, MediaType.APPLICATION_JSON_TYPE));

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        RoleDto fromServer = response.readEntity(RoleDto.class);

        assertThat(response, new CreatedResponseMatcher<>(fromServer, new IdentifiableDtoUriMatcher<>()));

        assertEquals(SecurityManagerIT.CUSTOM_TENANT_ROLE, fromServer.getName());

        roleUuid = fromServer.getId();

    }

    @Test
    public void test02GetList() throws Exception {

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        CollectionDto<RoleDto> collectionDto = IdentityManagementHelper.getRoles(token);

        assertThat(collectionDto.getItems(), iterableWithSize(ApplicationRole.getTenantRoles().size() + 1));
    }

    @Test
    public void test03Delete() throws Exception {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/role/{id}")
                .resolveTemplate("id", roleUuid)
                .request(MediaType.APPLICATION_JSON_TYPE);

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.DELETE, null))
                .delete();

        assertNotNull(response);
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    }

    @AfterClass
    public static void after() {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        IdentityManagementHelper.deleteTenant(token, uuid);

    }

}