package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.business.dto.UserDto;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.dematic.labs.business.SecurityFixture.*;
import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UserRoleGrantResourceIT extends SecuredEndpointFixture {

    private static String tenantUuid;
    private static List<RoleDto> roles;
    private static UserDto tenantUserDto;

    public UserRoleGrantResourceIT() throws MalformedURLException {
    }

    @BeforeClass
    public static void before() throws MalformedURLException {

        {
            SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

            //create tenant
            {
                Client client = ClientBuilder.newClient();
                WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant").toExternalForm()));

                TenantDto tenantDto = new TenantDto();
                tenantDto.setName(TENANT_A);

                Response response = signRequest(token, target.request()
                                .accept(MediaType.APPLICATION_JSON_TYPE)
                                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                        HttpMethod.POST, MediaType.APPLICATION_JSON
                ).post(Entity.entity(tenantDto, MediaType.APPLICATION_JSON_TYPE));

                assertNotNull(response);
                assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

                String location = response.getLocation().toString();
                String[] locationElements = location.split("/");
                tenantUuid = locationElements[locationElements.length-1];
            }

            //create tenant admin
            {
                Client client = ClientBuilder.newClient();
                WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenantAdminUser").toExternalForm()));

                UserDto userDto = new UserDto();
                TenantDto tenantDto = new TenantDto();
                tenantDto.setName(TENANT_A);
                userDto.setTenantDto(tenantDto);
                userDto.setLoginName(TENANT_A_ADMIN_USERNAME);
                userDto.setPassword(TENANT_A_ADMIN_PASSWORD);
                assertNull(userDto.getId());

                Response response = signRequest(token, target.request()
                                .accept(MediaType.APPLICATION_JSON_TYPE)
                                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                        HttpMethod.POST, MediaType.APPLICATION_JSON
                ).post(Entity.entity(userDto, MediaType.APPLICATION_JSON_TYPE));

                assertNotNull(response);
                assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

            }

        }

        {
            SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

            //create tenant user
            {
                Client client = ClientBuilder.newClient();
                WebTarget target = client.target(URI.create(new URL(getBase(), "resources/user").toExternalForm()));

                UserDto userDto = new UserDto();
                userDto.setLoginName(TENANT_A_USER_USERNAME);
                userDto.setPassword(TENANT_A_USER_PASSWORD);
                assertNull(userDto.getId());

                Response response = signRequest(token, target.request()
                                .accept(MediaType.APPLICATION_JSON_TYPE)
                                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                        HttpMethod.POST, MediaType.APPLICATION_JSON
                ).post(Entity.entity(userDto, MediaType.APPLICATION_JSON_TYPE));

                assertNotNull(response);
                assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

                tenantUserDto = response.readEntity(UserDto.class);

                assertNotNull(tenantUserDto);
                assertNotNull(tenantUserDto.getId());
                assertEquals(TENANT_A_USER_USERNAME, tenantUserDto.getLoginName());
            }

            //get roles
            {

                Client client = ClientBuilder.newClient();
                WebTarget target = client.target(URI.create(new URL(getBase(), "resources/role").toExternalForm()));

                roles = Arrays.asList(signRequest(token, target
                                .request(MediaType.APPLICATION_JSON)
                                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                        HttpMethod.GET, null
                ).get(RoleDto[].class));

                assertNotNull(roles);
                assertEquals(ApplicationRole.getTenantRoles().size(), roles.size());
            }
        }
    }

    @Test
    public void test010Grant() throws MalformedURLException {

        SignatureToken token = getToken(TENANT_A, TENANT_A_ADMIN_USERNAME, TENANT_A_ADMIN_PASSWORD);

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(
                URI.create(
                        new URL(getBase(), "resources/user/" + tenantUserDto.getId() + "/grant").toExternalForm()));

        assertEquals(0, tenantUserDto.getGrantedRoles().size());

        RoleDto roleDto = roles.stream()
                .filter(p -> p.getName().equals(ApplicationRole.VIEW_ORGANIZATIONS))
                .collect(Collectors.toList()).get(0);
        Set<RoleDto> grantedRoles = new HashSet<>();
        grantedRoles.add(roleDto);
        tenantUserDto.setGrantedRoles(grantedRoles);

        Response response = signRequest(token, target.request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.PUT, MediaType.APPLICATION_JSON
        ).put(Entity.entity(tenantUserDto, MediaType.APPLICATION_JSON_TYPE));

        assertNotNull(response);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

        UserDto fromServer = response.readEntity(UserDto.class);

        assertNotNull(fromServer);
        assertNotNull(fromServer.getGrantedRoles());
        assertEquals(1, fromServer.getGrantedRoles().size());
        assertEquals(ApplicationRole.VIEW_ORGANIZATIONS, fromServer.getGrantedRoles().iterator().next().getName());

    }

    @AfterClass
    public static void after() throws MalformedURLException {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant/" + tenantUuid).toExternalForm()));

        TenantDto tenantDto = new TenantDto();
        tenantDto.setName(TENANT_A);

        signRequest(token, target.request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.DELETE, null
        ).delete();

    }

}