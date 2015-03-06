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

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TokenResourceIT extends SecuredEndpointFixture {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    public TokenResourceIT() throws MalformedURLException {
    }

    @Test
    public void test1ModifiedBasicAuth() throws IOException {

        String tenant = "Dematic";
        String username = "superuser";
        String password = "abcd1234";

        SignatureToken token = getToken(tenant, username, password);

        assertNotNull(token);
        assertEquals(token.getSubject(), username);

        String signatureKey = token.getSignatureKey();

        assertFalse(Strings.isNullOrEmpty(signatureKey));
    }

    @Test
    public void test2BadTenant() throws IOException {

        String tenant = "Bad";
        String username = "superuser";
        String password = "abcd1234";

        exception.expect(NotAuthorizedException.class);
        getToken(tenant, username, password);
    }

    @Test
    public void test3BadUsername() throws IOException {

        String tenant = "Dematic";
        String username = "Bad";
        String password = "abcd1234";

        exception.expect(NotAuthorizedException.class);
        getToken(tenant, username, password);
    }

    @Test
    public void test4BadPassword() throws IOException {

        String tenant = "Dematic";
        String username = "superuser";
        String password = "Bad";

        exception.expect(NotAuthorizedException.class);
        getToken(tenant, username, password);
    }

    @Test
    public void test5ModifiedBasicAuthWrongUri() throws IOException {

        String tenant = "Dematic";
        String username = "superuser";
        String password = "abcd1234";

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/role").toExternalForm()));
        Response response = target.request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        generateBasicAuthHeaderValue(tenant, username, password))
                .get();
        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
    }

}