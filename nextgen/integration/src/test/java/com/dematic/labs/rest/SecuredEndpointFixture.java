package com.dematic.labs.rest;

import com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import org.jboss.resteasy.client.jaxrs.internal.ClientInvocation;
import org.jboss.resteasy.client.jaxrs.internal.ClientRequestHeaders;

import javax.annotation.Nonnull;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme.generateStringToSign;
import static com.dematic.labs.http.picketlink.authentication.schemes.DLabsAuthenticationScheme.sign;
import static org.junit.Assert.assertEquals;

public abstract class SecuredEndpointFixture {

    public static final String SCHEME = "http";
    public static final String HOSTNAME = "localhost:8080";
    public static final String CONTEXT_ROOT = "/admin/";

    public SecuredEndpointFixture() {
    }

    public static URL getBase() {
        try {
            return new URL(SCHEME + "://" + HOSTNAME + CONTEXT_ROOT);

        } catch (MalformedURLException e) {
            return null;
        }
    }

    protected static SignatureToken getToken(String tenant, String username, String password) throws MalformedURLException {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(URI.create(new URL(getBase(), "resources/token").toExternalForm()));
        return target.request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                        generateBasicAuthHeaderValue(tenant, username, password))
                .get(SignatureToken.class);
    }

    private static String generateBasicAuthHeaderValue(String tenant, String username, String password) {
        //TODO - add base64 encoding
        return "DLabsU " + tenant + ":" + username + ":" + password;
    }

    protected static String generateSignatureAuthHeaderValue(String tenant, String username, String signature) {
        return "DLabsT " + tenant + ":" + username + ":" + signature;
    }

    protected static Invocation.Builder signRequest(@Nonnull SignatureToken token, @Nonnull Invocation.Builder request, @Nonnull String httpMethod, String postPutContentType) {

        String stringToSign = extractStringToSignFromRequest(httpMethod, postPutContentType, request);

        String signature = sign(stringToSign, token.getSignatureKey());

        request.header(DLabsAuthenticationScheme.AUTHORIZATION_HEADER_NAME,
                generateSignatureAuthHeaderValue(token.getRealm(), token.getSubject(), signature));

        return request;
    }

    protected static String extractStringToSignFromRequest(String httpMethod, String postPutContentType, Invocation.Builder request) {
        SortedMap<String, String> queryParameters = new TreeMap<>();
        SortedMap<String, String> canonicalHeaders = new TreeMap<>();
        SortedMap<String, String> dLabsHeaders = new TreeMap<>();

        ClientInvocation invocation = (ClientInvocation) request.build(httpMethod);

        String uri = invocation.getUri().getPath();

        String queryString = invocation.getUri().getQuery();
        if (queryString != null) {
            for (String nameValuePair : queryString.split("&")) {
                String[] parts = nameValuePair.split("=");
                queryParameters.put(parts[0], parts[1]);
            }
        }

        ClientRequestHeaders headers = invocation.getHeaders();

        canonicalHeaders.put("Content-MD5", headers.getHeader("Content-MD5"));
        if ((httpMethod.equals(HttpMethod.POST) || httpMethod.equals(HttpMethod.PUT)) && postPutContentType != null) {
            canonicalHeaders.put("Content-Type", postPutContentType);
        } else {
            canonicalHeaders.put("Content-Type", headers.getHeader("Content-Type"));
        }
        canonicalHeaders.put("Date", headers.getHeader("Date"));

        if (headers.getHeader(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME) != null) {
            canonicalHeaders.put("Date", headers.getHeader(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME));
        } else {
            canonicalHeaders.put("Date", headers.getHeader("Date"));
        }

        for (Map.Entry<String, List<String>> entry : headers.asMap().entrySet()) {
            String headerName = entry.getKey();
            if (headerName.startsWith(DLabsAuthenticationScheme.D_LABS_HEADER_PREFIX)
                    && !headerName.equals(DLabsAuthenticationScheme.D_LABS_DATE_HEADER_NAME)) {
                assertEquals(1, entry.getValue().size());
                dLabsHeaders.put(headerName, headers.getHeader(entry.getValue().get(0)));
            }
        }

        return generateStringToSign(httpMethod, uri, queryParameters, canonicalHeaders, dLabsHeaders);
    }

}