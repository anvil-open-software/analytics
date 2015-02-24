package com.dematic.labs.rest;

import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import org.jboss.resteasy.client.jaxrs.internal.ClientInvocation;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SecuredResourceIT extends SecuredEndpointFixture {

    public SecuredResourceIT() throws MalformedURLException {
    }

    @Test
    public void test1TokenSignatureAuth() throws IOException {

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));

            Response response = signRequest(token, target.request()
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.GET,
                    null).get();

            assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

        }

    }

    @Test
    public void test2StaleRequest() throws IOException {

        String staleRequestTimestamp = Instant.now().minus(Duration.ofMinutes(20)).toString();

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, staleRequestTimestamp),
                    HttpMethod.GET,
                    null).get();

            assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

        }

    }

    @Test
    public void test3BadTenant() throws IOException {

        token.setRealm("Bad");
        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.GET,
                    null).get();

            assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

        }

    }

    @Test
    public void test4BadUsername() throws IOException {

        token.setToken("Bad");
        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.GET,
                    null).get();

            assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

        }

    }

    @Test
    public void test5BadSignatureKey() throws IOException {

        token.setSignatureKey("Bad");
        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.GET,
                    null).get();

            assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

        }

    }

    @Test
    public void test6MismatchedHttpMethod() throws IOException {

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));

            Response response = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.PUT,
                    null).get();

            assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

        }

    }

    @Test
    public void test7MismatchedTimestamp() throws IOException {

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal").toExternalForm()));

            Invocation.Builder request = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.GET,
                    null);


            Response response = request
                    .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, null)
                    .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME,
                            Instant.now().minusSeconds(10).toString())
                    .get();

            assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

        }

    }

    @Test
    public void test8MismatchedURI() throws IOException {

        {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(URI.create(new URL(getBase(), "resources/principal?offset=25&records=50").toExternalForm()));

            Invocation.Builder request = signRequest(token, target.request()
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString()),
                    HttpMethod.GET,
                    null);

            ClientInvocation invocation = (ClientInvocation) request.buildGet();
            invocation.setUri(URI.create(new URL(getBase(), "resources/realm?records=50&offset=25").toExternalForm()));
            Response response = invocation.invoke();

            assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

        }

    }

}