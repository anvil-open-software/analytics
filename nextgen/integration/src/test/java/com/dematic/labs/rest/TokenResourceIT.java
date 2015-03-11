package com.dematic.labs.rest;

import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.google.common.base.Strings;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TokenResourceIT extends SecuredEndpointFixture {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    public TokenResourceIT() throws MalformedURLException {
    }

    @Test
    public void test1ModifiedBasicAuth() throws IOException {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        assertNotNull(token);
        assertEquals(INSTANCE_ADMIN_USERNAME, token.getSubject());

        String signatureKey = token.getSignatureKey();

        assertFalse(Strings.isNullOrEmpty(signatureKey));
    }

    @Test
    public void test2BadTenant() throws IOException {

        exception.expect(NotAuthorizedException.class);
        getToken("Bad", INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
    }

    @Test
    public void test3BadUsername() throws IOException {

        exception.expect(NotAuthorizedException.class);
        getToken(INSTANCE_TENANT_NAME, "Bad", INSTANCE_ADMIN_PASSWORD);
    }

    @Test
    public void test4BadPassword() throws IOException {

        exception.expect(NotAuthorizedException.class);
        getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, "Bad");
    }

    @Test
    public void test5ModifiedBasicAuthWrongUri() throws IOException {

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/role").toExternalForm()));
        Response response = target.request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        generateBasicAuthHeaderValue(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD))
                .get();
        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
    }

}