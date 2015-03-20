package com.dematic.labs.rest;

import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import org.jboss.resteasy.client.jaxrs.internal.ClientInvocation;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;

import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.*;
import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SecuredResourceIT {

    public SecuredResourceIT() {
    }

    @Test
    public void test1TokenSignatureAuth() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .request(MediaType.APPLICATION_JSON_TYPE);

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get();

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    }

    @Test
    public void test2StaleRequest() {

        String staleRequestTimestamp = Instant.now().minus(Duration.ofMinutes(20)).toString();

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .request(MediaType.APPLICATION_JSON_TYPE);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, staleRequestTimestamp)
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get();

        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

    }

    @Test
    public void test3BadTenant() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .request(MediaType.APPLICATION_JSON_TYPE);

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        token.setRealm("Bad");

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get();

        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

    }

    @Test
    public void test4BadUsername() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .request(MediaType.APPLICATION_JSON_TYPE);

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        token.setToken("Bad");

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get();

        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

    }

    @Test
    public void test5BadSignatureKey() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .request(MediaType.APPLICATION_JSON_TYPE);

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
        token.setSignatureKey("Bad");

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get();

        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

    }

    @Test
    public void test6MismatchedHttpMethod() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .request(MediaType.APPLICATION_JSON_TYPE);

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.PUT, null))
                .get();

        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

    }

    @Test
    public void test7MismatchedTimestamp() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/tenant")
                .request(MediaType.APPLICATION_JSON_TYPE);

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        request.header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null));

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, null)
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME,
                        Instant.now().minusSeconds(10).toString())
                .get();

        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

    }

    @Test
    public void test8MismatchedURI() throws MalformedURLException {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/role?offset=25&records=50")
                .request(MediaType.APPLICATION_JSON_TYPE);

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        request.header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null));

        ClientInvocation invocation = (ClientInvocation) request.buildGet();
        invocation.setUri(URI.create(new URL(getBase(), "resources/tenant?records=50&offset=25").toExternalForm()));
        Response response = invocation.invoke();

        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());

    }

    @Test
    public void test9TokenSignatureAuthWrongUri() {

        Invocation.Builder request = ClientBuilder.newClient().target(BASE_URL)
                .path("resources/token")
                .request(MediaType.APPLICATION_JSON_TYPE);

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        Response response = request
                .header(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME, Instant.now().toString())
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        signRequest(request, token, HttpMethod.GET, null))
                .get();

        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
    }

}