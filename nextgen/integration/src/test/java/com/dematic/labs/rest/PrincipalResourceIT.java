package com.dematic.labs.rest;

import com.dematic.labs.business.dto.PrincipalDto;
import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
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
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Instant;

import static com.dematic.labs.picketlink.SecurityInitializer.INSTANCE_ADMIN_PASSWORD;
import static com.dematic.labs.picketlink.SecurityInitializer.INSTANCE_ADMIN_USERNAME;
import static com.dematic.labs.picketlink.SecurityInitializer.INSTANCE_TENANT_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PrincipalResourceIT extends SecuredEndpointFixture {

    private static SignatureToken token;

    public PrincipalResourceIT() throws MalformedURLException {
    }

    @BeforeClass
    public static void before() throws MalformedURLException {
        token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
    }

    @Test
    public void test1PostViaForm() throws MalformedURLException {

        String uuid;

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));

            MultivaluedHashMap<String, String> map = new MultivaluedHashMap<>();
            map.add("userName", "Penny");

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.POST, MediaType.APPLICATION_FORM_URLENCODED
                    ).post(Entity.form(map));

            assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

            String location = response.getLocation().toString();
            String[] locationElements = location.split("/");
            uuid = locationElements[locationElements.length-1];
        }

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));
            target.register(PrincipalDto.class);

            PrincipalDto p = signRequest(token, target
                            .path("{id}")
                            .resolveTemplate("id", uuid)
                            .request(MediaType.APPLICATION_XML)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.GET, null)
                    .get(PrincipalDto.class);

            assertNotNull(p);
        }

    }

    @Test
    public void test2PostViaDto() throws MalformedURLException {

        String uuid;

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));

            PrincipalDto principalDto = new PrincipalDto();
            principalDto.setUsername("Fred");

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.POST, MediaType.APPLICATION_XML
                    ).post(Entity.entity(principalDto, MediaType.APPLICATION_XML_TYPE));

            assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());

            String location = response.getLocation().toString();
            String[] locationElements = location.split("/");
            uuid = locationElements[locationElements.length-1];
        }

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));
            target.register(PrincipalDto.class);

            PrincipalDto p = signRequest(token, target
                            .path("{id}")
                            .resolveTemplate("id", uuid)
                            .request(MediaType.APPLICATION_XML)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.GET,
                    null).get(PrincipalDto.class);

            assertNotNull(p);
        }

    }

    @Test
    public void test3GetList() throws Exception {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));

        PrincipalDto[] list = signRequest(token, target
                        .request(MediaType.APPLICATION_XML)
                        .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                HttpMethod.GET, null
                ).get(PrincipalDto[].class);

        assertEquals(2, list.length);
    }
}