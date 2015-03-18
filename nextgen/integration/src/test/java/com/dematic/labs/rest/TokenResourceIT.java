package com.dematic.labs.rest;

import com.dematic.labs.business.ApplicationRole;
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
import java.net.MalformedURLException;

import static com.dematic.labs.picketlink.SecurityInitializer.*;
import static com.dematic.labs.rest.SecuredEndpointHelper.*;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TokenResourceIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    public TokenResourceIT() throws MalformedURLException {
    }

    @Test
    public void test1ModifiedBasicAuth() {

        SignatureToken token = getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);

        assertNotNull(token);
        assertEquals(INSTANCE_ADMIN_USERNAME, token.getSubject());

        String signatureKey = token.getSignatureKey();

        assertFalse(Strings.isNullOrEmpty(signatureKey));

        assertThat(token.getGrantedRoles(), containsInAnyOrder(ApplicationRole.ADMINISTER_TENANTS));
    }

    @Test
    public void test2BadTenant() {

        exception.expect(NotAuthorizedException.class);
        getToken("Bad", INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD);
    }

    @Test
    public void test3BadUsername() {

        exception.expect(NotAuthorizedException.class);
        getToken(INSTANCE_TENANT_NAME, "Bad", INSTANCE_ADMIN_PASSWORD);
    }

    @Test
    public void test4BadPassword() {

        exception.expect(NotAuthorizedException.class);
        getToken(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, "Bad");
    }

    @Test
    public void test5ModifiedBasicAuthWrongUri()  {

        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(BASE_URL);
        Response response = target
                .path("resources/role")
                .request(MediaType.APPLICATION_JSON_TYPE)
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        generateBasicAuthHeaderValue(INSTANCE_TENANT_NAME, INSTANCE_ADMIN_USERNAME, INSTANCE_ADMIN_PASSWORD))
                .get();
        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
    }

}