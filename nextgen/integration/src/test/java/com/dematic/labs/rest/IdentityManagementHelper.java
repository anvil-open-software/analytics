package com.dematic.labs.rest;

import com.dematic.labs.business.dto.CollectionDto;
import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.business.matchers.NamedDtoMatcher;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.matchers.CreatedResponseMatcher;
import com.dematic.labs.rest.matchers.IdentifiableDtoUriMatcher;
import com.dematic.labs.rest.matchers.UserDtoUriMatcher;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.dematic.labs.rest.SecuredEndpointHelper.BASE_URL;
import static com.dematic.labs.rest.SecuredEndpointHelper.signRequest;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.*;

public class IdentityManagementHelper {

    protected static TenantDto createTenant(SignatureToken token, String tenantName) {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .request(MediaType.APPLICATION_JSON_TYPE);

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(tenantName);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.POST, MediaType.APPLICATION_JSON))
                .post(Entity.entity(tenantDto, MediaType.APPLICATION_JSON_TYPE));

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        TenantDto fromServer = response.readEntity(TenantDto.class);
        assertThat(response, new CreatedResponseMatcher<>(fromServer, new IdentifiableDtoUriMatcher<>()));

        assertEquals(tenantName, fromServer.getName());

        return fromServer;
    }

    protected static void deleteTenant(SignatureToken token, String tenantId) {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant/{id}")
                .resolveTemplate("id", tenantId)
                .request(MediaType.APPLICATION_JSON_TYPE);

        request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.DELETE, null))
                .delete();
    }

    protected static UserDto createTenantAdmin(SignatureToken token,
                                               String tenantName, String username, String password) {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenantAdminUser")
                .request(MediaType.APPLICATION_JSON_TYPE);

        UserDto userDto = new UserDto();
        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(tenantName);
        userDto.setTenantDto(tenantDto);
        userDto.setLoginName(username);
        userDto.setPassword(password);
        assertNull(userDto.getId());

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.POST, MediaType.APPLICATION_JSON))
                .post(Entity.entity(userDto, MediaType.APPLICATION_JSON_TYPE));

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        UserDto fromServer = response.readEntity(UserDto.class);
        assertThat(response, new CreatedResponseMatcher<>(fromServer, new UserDtoUriMatcher()));

        assertEquals(username, fromServer.getLoginName());
        return fromServer;

    }

    protected static UserDto createTenantUser(SignatureToken token,
                                              String username, String password) {
        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/user")
                .request(MediaType.APPLICATION_JSON_TYPE);

        UserDto userDto = new UserDto();
        userDto.setLoginName(username);
        userDto.setPassword(password);
        assertNull(userDto.getId());

        Response response = request
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.POST, MediaType.APPLICATION_JSON))
                .post(Entity.entity(userDto, MediaType.APPLICATION_JSON_TYPE));

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        UserDto fromServer = response.readEntity(UserDto.class);
        assertThat(response, new CreatedResponseMatcher<>(fromServer, new UserDtoUriMatcher()));

        assertEquals(username, fromServer.getLoginName());
        return fromServer;
    }

    public static CollectionDto<RoleDto> getRoles(@Nonnull SignatureToken token) {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/role")
                .request(MediaType.APPLICATION_JSON_TYPE);

        CollectionDto<RoleDto> roles = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get(new GenericType<CollectionDto<RoleDto>>() {});

        assertThat(roles.getItems(), not(empty()));
        assertThat(roles.getItems(), everyItem(new IdentifiableDtoUriMatcher<>()));
        return roles;
    }

    public static UserDto grantRoles(SignatureToken token, UserDto userDto, String... roleNames) {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/user/{id}/grant")
                .resolveTemplate("id", userDto.getId())
                .request(MediaType.APPLICATION_JSON_TYPE);

        List<String> roleNameList = Arrays.asList(roleNames);
        Set<RoleDto> rolesToGrant = IdentityManagementHelper.getRoles(token).getItems().stream()
                .filter(p -> roleNameList.contains(p.getName()))
                .collect(Collectors.toSet());
        userDto.setGrantedRoles(rolesToGrant);

        Response response = request
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.PUT, MediaType.APPLICATION_JSON))
                .put(Entity.entity(userDto, MediaType.APPLICATION_JSON_TYPE));

        assertNotNull(response);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        UserDto fromServer = response.readEntity(UserDto.class);

        assertThat(fromServer, new UserDtoUriMatcher());
        assertThat(fromServer.getGrantedRoles(), iterableWithSize(roleNameList.size()));

        assertThat(fromServer.getGrantedRoles(), everyItem(new IdentifiableDtoUriMatcher<>()));

        //noinspection RedundantTypeArguments
        List<Matcher<? super RoleDto>> matcherList = roleNameList.stream()
                .map(NamedDtoMatcher<RoleDto>::new).collect(Collectors.toList());
        assertThat(fromServer.getGrantedRoles(), containsInAnyOrder(matcherList));

        return fromServer;
    }
}
