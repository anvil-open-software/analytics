package com.dematic.labs.rest;

import com.dematic.labs.business.SecurityManagerIT;
import com.dematic.labs.business.dto.TenantDto;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.rest.dto.RestError;
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

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TenantResourceIT extends SecuredEndpointFixture {

    public TenantResourceIT() throws MalformedURLException {
    }

    @Test
    public void test01GetList() throws Exception {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant").toExternalForm()));

        TenantDto[] list = signRequest(token, target
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
        ).get(TenantDto[].class);

        assertNotNull(list);
        assertTrue(list.length > 0);
    }

    /*
     Need one test of failing authorization to cover 1) Picketlink SecurityInterceptor is wired correctly and
     2) that ExceptionMapping is wired correctly
     */
    @Test
    public void test02GetListWithoutAuthorization() throws Exception {

        SignatureToken userToken = getToken("Safeway", "joeUser", "abcd1234");

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant").toExternalForm()));

        Response response = signRequest(userToken, target
                        .request(MediaType.APPLICATION_JSON)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
        ).get();

        assertNotNull(response);
        assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());

        RestError error = response.readEntity(RestError.class);
        assertNotNull(error);
        assertEquals(Response.Status.FORBIDDEN.getStatusCode(), error.getHttpStatusCode());
    }

    @Test
    public void test03Create() throws MalformedURLException {

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant").toExternalForm()));

            TenantDto tenantDto = new TenantDto();
            tenantDto.setName(SecurityManagerIT.NEW_TENANT);

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.POST, MediaType.APPLICATION_JSON
            ).post(Entity.entity(tenantDto, MediaType.APPLICATION_JSON_TYPE));

            assertNotNull(response);
            assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

            String location = response.getLocation().toString();
            String[] locationElements = location.split("/");
            String uuid = locationElements[locationElements.length-1];

            TenantDto fromServer = response.readEntity(TenantDto.class);

            assertNotNull(fromServer);
            assertNotNull(fromServer.getId());
            assertFalse(fromServer.getId().isEmpty());
            assertEquals(uuid, fromServer.getId());
        }

    }

    @Test
    public void test04CreateDuplicate() throws MalformedURLException {

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant").toExternalForm()));

            TenantDto tenantDto = new TenantDto();
            tenantDto.setName(SecurityManagerIT.NEW_TENANT);

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.POST, MediaType.APPLICATION_JSON
            ).post(Entity.entity(tenantDto, MediaType.APPLICATION_JSON_TYPE));

            assertNotNull(response);
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());

            RestError error = response.readEntity(RestError.class);
            assertNotNull(error);
            assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), error.getHttpStatusCode());
            assertEquals("Duplicate Tenant Name", error.getMessage());
        }

    }

}