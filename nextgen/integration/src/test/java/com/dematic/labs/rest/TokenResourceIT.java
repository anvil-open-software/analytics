package com.dematic.labs.rest;

import com.dematic.labs.business.picketlink.idm.credential.SignatureToken;
import com.google.common.base.Strings;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;

import javax.ws.rs.NotAuthorizedException;
import java.io.IOException;
import java.net.MalformedURLException;

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

}