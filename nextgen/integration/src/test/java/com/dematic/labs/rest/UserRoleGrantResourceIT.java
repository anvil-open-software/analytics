package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.business.matchers.NamedDtoMatcher;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.matchers.UserDtoUriMatcher;
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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.*;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UserRoleGrantResourceIT {

    private static String tenantUuid;
    private static List<RoleDto> roles;
    private static UserDto tenantUserDto;

    public UserRoleGrantResourceIT() {
    }

    @BeforeClass
    public static void before() {

        {
            SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

            tenantUuid = IdentityManagementHelper.createTenant(token, TENANT_A).getId();

            IdentityManagementHelper.createTenantAdmin(token, TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        }

        {
            SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

            tenantUserDto = IdentityManagementHelper.createTenantUser(token, TENANT_A_USER_USERNAME, TENANT_A_USER_PASSWORD);

            //get roles
            roles = IdentityManagementHelper.getRoles(token).getItems();
            assertThat(roles, iterableWithSize(ApplicationRole.getTenantRoles().size()));
        }
    }

    @Test
    public void test010Grant() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/user/{id}/grant")
                .resolveTemplate("id", tenantUserDto.getId())
                .request(MediaType.APPLICATION_JSON_TYPE);

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        assertEquals(0, tenantUserDto.getGrantedRoles().size());

        List<String> grantedRolesNames = Arrays.asList(ApplicationRole.VIEW_ORGANIZATIONS);
        Set<RoleDto> grantedRoles = roles.stream()
                .filter(p->grantedRolesNames.contains(p.getName()))
                .collect(Collectors.toSet());
        tenantUserDto.setGrantedRoles(grantedRoles);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.PUT, MediaType.APPLICATION_JSON))
                .put(Entity.entity(tenantUserDto, MediaType.APPLICATION_JSON_TYPE));

        assertNotNull(response);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        UserDto fromServer = response.readEntity(UserDto.class);

        assertThat(fromServer, new UserDtoUriMatcher());
        assertThat(fromServer.getGrantedRoles(), iterableWithSize(grantedRolesNames.size()));
        assertThat(fromServer.getGrantedRoles(), contains(new NamedDtoMatcher<>(ApplicationRole.VIEW_ORGANIZATIONS)));

    }

    @AfterClass
    public static void after() {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        IdentityManagementHelper.deleteTenant(token, tenantUuid);

    }

}