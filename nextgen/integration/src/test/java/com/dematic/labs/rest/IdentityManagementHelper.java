package com.dematic.labs.rest;

import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.rest.matchers.CreatedResponseMatcher;
import com.dematic.labs.rest.matchers.IdentifiableDtoHrefMatcher;
import com.dematic.labs.rest.matchers.UserDtoHrefMatcher;

import javax.annotation.Nonnull;
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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.dematic.labs.business.SecurityFixture.TENANT_A;
import static com.dematic.labs.rest.SecuredEndpointHelper.getBase;
import static com.dematic.labs.rest.SecuredEndpointHelper.signRequest;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.*;

public class IdentityManagementHelper {

    protected static TenantDto createTenant(SignatureToken token, String tenantName) throws MalformedURLException {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant").toExternalForm()));

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(tenantName);

        Response response = signRequest(token, target.request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.POST, MediaType.APPLICATION_JSON
        ).post(Entity.entity(tenantDto, MediaType.APPLICATION_JSON_TYPE));

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        TenantDto fromServer = response.readEntity(TenantDto.class);
        assertThat(response, new CreatedResponseMatcher<>(fromServer, new IdentifiableDtoHrefMatcher<>()));

        assertEquals(tenantName, fromServer.getName());

        return fromServer;
    }

    protected static void deleteTenant(SignatureToken token, String tenantId) throws MalformedURLException {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(
                new URL(getBase(), "resources/tenant/" + tenantId).toExternalForm()));

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(TENANT_A);

        signRequest(token, target.request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.DELETE, null
        ).delete();


    }

    protected static UserDto createTenantAdmin(SignatureToken token,
                                               String tenantName, String username, String password) throws MalformedURLException {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenantAdminUser").toExternalForm()));

        UserDto userDto = new UserDto();
        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(tenantName);
        userDto.setTenantDto(tenantDto);
        userDto.setLoginName(username);
        userDto.setPassword(password);
        assertNull(userDto.getId());

        Response response = signRequest(token, target.request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.POST, MediaType.APPLICATION_JSON
        ).post(Entity.entity(userDto, MediaType.APPLICATION_JSON_TYPE));

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        UserDto fromServer = response.readEntity(UserDto.class);
        assertThat(response, new CreatedResponseMatcher<>(fromServer, new UserDtoHrefMatcher()));

        assertEquals(username, fromServer.getLoginName());
        return fromServer;

    }

    protected static UserDto createTenantUser(SignatureToken token,
                                              String username, String password) throws MalformedURLException {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/user").toExternalForm()));

        UserDto userDto = new UserDto();
        userDto.setLoginName(username);
        userDto.setPassword(password);
        assertNull(userDto.getId());

        Response response = signRequest(token, target.request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.POST, MediaType.APPLICATION_JSON
        ).post(Entity.entity(userDto, MediaType.APPLICATION_JSON_TYPE));

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        UserDto fromServer = response.readEntity(UserDto.class);
        assertThat(response, new CreatedResponseMatcher<>(fromServer, new UserDtoHrefMatcher()));

        assertEquals(username, fromServer.getLoginName());
        return fromServer;
    }

    public static List<RoleDto> getRoles(@Nonnull SignatureToken token) throws MalformedURLException {
        List<RoleDto> roles;
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/role").toExternalForm()));

        roles = signRequest(token, target
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
        ).get(new GenericType<List<RoleDto>>() {});

        assertThat(roles, not(empty()));
        assertThat(roles, everyItem(new IdentifiableDtoHrefMatcher<>()));
        return roles;
    }

    public static UserDto grantRoles(SignatureToken token, UserDto userDto, String... roleNames) throws MalformedURLException {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(
                URI.create(
                        new URL(getBase(), "resources/user/" + userDto.getId() + "/grant").toExternalForm()));

        assertEquals(0, userDto.getGrantedRoles().size());

        List<String> roleNameList = Arrays.asList(roleNames);
        userDto.setGrantedRoles(IdentityManagementHelper.getRoles(token).stream()
                .filter(p -> roleNameList.contains(p.getName()))
                .collect(Collectors.toSet()));

        Response response = signRequest(token, target.request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.PUT, MediaType.APPLICATION_JSON
        ).put(Entity.entity(userDto, MediaType.APPLICATION_JSON_TYPE));

        assertNotNull(response);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        UserDto fromServer = response.readEntity(UserDto.class);

        assertThat(fromServer, new UserDtoHrefMatcher());
        assertThat(fromServer.getGrantedRoles(), iterableWithSize(roleNameList.size()));
        return fromServer;
    }
}
