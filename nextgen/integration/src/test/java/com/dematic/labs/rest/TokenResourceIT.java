package com.dematic.labs.rest;

import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.google.common.base.Strings;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;

import javax.ws.rs.NotAuthorizedException;
import java.io.IOException;
import java.net.MalformedURLException;

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

}