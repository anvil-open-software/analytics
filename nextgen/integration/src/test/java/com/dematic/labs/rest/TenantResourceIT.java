package com.dematic.labs.rest;

import com.dematic.labs.business.dto.PrincipalDto;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
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

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Ignore
public class TenantResourceIT extends SecuredEndpointFixture {

    public TenantResourceIT() throws MalformedURLException {
    }

    @Test
    public void test01PostViaDto() throws MalformedURLException {

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant").toExternalForm()));

            PrincipalDto principalDto = new PrincipalDto();
            principalDto.setUsername("Fred");

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.POST, MediaType.APPLICATION_XML
                    ).post(Entity.entity(principalDto, MediaType.APPLICATION_XML_TYPE));

            assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

        }

   }

    @Test
    public void test02GetList() throws Exception {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant").toExternalForm()));

        PrincipalDto[] list = signRequest(token, target
                        .request(MediaType.APPLICATION_XML)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
                ).get(PrincipalDto[].class);

        assertEquals(2, list.length);
    }

    @Test
    public void test03GetListWithoutAuthorization() throws Exception {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/tenant").toExternalForm()));

        PrincipalDto[] list = signRequest(token, target
                        .request(MediaType.APPLICATION_XML)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
        ).get(PrincipalDto[].class);

        assertEquals(2, list.length);
    }

}