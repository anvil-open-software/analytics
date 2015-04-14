package com.dematic.labs.http.picketlink.authentication.schemes;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.util.Base64;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.picketlink.idm.credential.SignatureTokenCredential;
import com.dematic.labs.picketlink.RealmSelector;
import com.dematic.labs.picketlink.idm.credential.SignatureTokenProvider;
import org.picketlink.Identity;
import org.picketlink.authentication.AuthenticationException;
import org.picketlink.common.util.StringUtil;
import org.picketlink.config.http.BasicAuthenticationConfiguration;
import org.picketlink.credential.DefaultLoginCredentials;
import org.picketlink.http.authentication.HttpAuthenticationScheme;
import org.picketlink.idm.credential.Token;
import org.picketlink.idm.model.Account;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.Instant;
import java.util.Enumeration;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.amazonaws.util.StringUtils.UTF8;
import static org.picketlink.http.internal.util.RequestUtil.isAjaxRequest;
import static org.picketlink.idm.credential.Token.Builder.create;

public class DLabsAuthenticationScheme implements HttpAuthenticationScheme<BasicAuthenticationConfiguration> {

    public static final String DEFAULT_REALM_NAME = "PicketLink Default Realm";

    private String realm = DEFAULT_REALM_NAME;

    public static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    public static final String USERNAME_AUTHENTICATION_SCHEME_NAME = "DLabsU";
    public static final String TOKEN_AUTHENTICATION_SCHEME_NAME = "DLabsT";
    public static final String D_LABS_HEADER_PREFIX = "x-dlabs";
    public static final String D_LABS_DATE_HEADER_NAME = D_LABS_HEADER_PREFIX + "-date";

    @Inject
    RealmSelector realmSelector;

    @Inject
    private Instance<Identity> identityInstance;

    @Inject
    private Instance<DefaultLoginCredentials> credentialsInstance;

    @Inject
    private Instance<Token.Provider<?>> tokenProvider;

    @Inject
    private Instance<Token.Consumer<?>> tokenConsumer;

    @Override
    public void initialize(BasicAuthenticationConfiguration config) {
        String providedRealm = config.getRealmName();

        if (providedRealm != null) {
            this.realm = providedRealm;
        }
    }

    @Override
    public void extractCredential(HttpServletRequest request, DefaultLoginCredentials creds) {

        if (isDLabsAuthentication(request, USERNAME_AUTHENTICATION_SCHEME_NAME) && isGetTokenRequest(request)) {

            String[] realmUsernameAndPassword = extractRealmAndCredentials(request, USERNAME_AUTHENTICATION_SCHEME_NAME);

            if (realmUsernameAndPassword.length == 3) {
                String realm = realmUsernameAndPassword[0];
                String username = realmUsernameAndPassword[1];
                String password = realmUsernameAndPassword[2];

                if(!StringUtil.isNullOrEmpty(realm)) {
                    realmSelector.setRealm(realm);
                }

                if (!(StringUtil.isNullOrEmpty(username) && StringUtil.isNullOrEmpty(password))) {
                    creds.setUserId(username);
                    creds.setPassword(password);
                }
            }
        } else if (isDLabsAuthentication(request, TOKEN_AUTHENTICATION_SCHEME_NAME) && !isGetTokenRequest(request)) {

            // if credentials are not present, we try to extract the token from the request.
            String[] realmUsernameAndSignature = extractRealmAndCredentials(request, TOKEN_AUTHENTICATION_SCHEME_NAME);
            String stringToSign = extractStringToSignFromRequest(request);
            Instant requestTimestamp = extractTimestampFromRequest(request);

            if (realmUsernameAndSignature.length == 3) {

                String realm = realmUsernameAndSignature[0];
                String username = realmUsernameAndSignature[1];
                String signature = realmUsernameAndSignature[2];

                if(!StringUtil.isNullOrEmpty(realm)) {
                    realmSelector.setRealm(realm);
                }

                if (!(StringUtil.isNullOrEmpty(username) && StringUtil.isNullOrEmpty(signature)
                        && StringUtil.isNullOrEmpty(stringToSign))) {
                    creds.setCredential(createCredential(username, signature, stringToSign, requestTimestamp));
                }
            }
        }
    }

    private boolean isGetTokenRequest(HttpServletRequest request) {
        return request.getMethod().equals(HttpMethod.GET) && request.getRequestURI().endsWith("token");
    }

    @Override
    public void challengeClient(HttpServletRequest request, HttpServletResponse response) {

        try {
            //response.setHeader("WWW-Authenticate", "Basic realm=\"" + this.realm + "\"");
            response.setHeader("WWW-Authenticate", "XXXXX realm=\"" + this.realm + "\"");

            // this usually means we have a failing authentication request from an ajax client. so we return SC_FORBIDDEN instead.
            // this is a workaround to avoid browsers to popup an authentication dialog when authentication via ajax.
            if (isAjaxRequest(request)) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
            } else {
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not challenge client credential.", e);
        }
    }

    @Override
    public void onPostAuthentication(HttpServletRequest request, HttpServletResponse response) {
        if (getIdentity().isLoggedIn()) {
            if (isPrimaryAuthenticationRequest()) {
                issueToken(getIdentity().getAccount());
            } else {
                renewToken(getIdentity().getAccount(), ((SignatureTokenCredential) getCredentials().getCredential()).getToken());
            }
        }
    }

    private boolean isDLabsAuthentication(HttpServletRequest request, String scheme) {
        return getAuthorizationHeader(request) != null && getAuthorizationHeader(request).startsWith(scheme);
    }

    private String getAuthorizationHeader(HttpServletRequest request) {
        return request.getHeader(AUTHORIZATION_HEADER_NAME);
    }

    public String[] extractRealmAndCredentials(HttpServletRequest request, String scheme) {
        String authorizationHeader = getAuthorizationHeader(request);

        if (authorizationHeader != null && authorizationHeader.startsWith(scheme)) {
            String token = authorizationHeader.substring(scheme.length() + 1);
            return token.split(":");
        }

        return null;

    }

    protected String extractStringToSignFromRequest(HttpServletRequest request) {
        SortedMap<String, String> queryParameters = new TreeMap<>();
        SortedMap<String, String> canonicalHeaders = new TreeMap<>();
        SortedMap<String, String> dLabsHeaders = new TreeMap<>();

        String uri = request.getRequestURI();

        String queryString = request.getQueryString();
        if (queryString != null) {
            try {
                queryString = URLDecoder.decode(queryString, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            for (String nameValuePair : queryString.split("&")) {
                String[] parts = nameValuePair.split("=");
                queryParameters.put(parts[0], parts[1]);
            }
        }

        canonicalHeaders.put("Content-MD5", request.getHeader("Content-MD5"));
        canonicalHeaders.put("Content-Type", request.getHeader("Content-Type"));

        if (request.getHeader(D_LABS_DATE_HEADER_NAME) != null) {
            canonicalHeaders.put("Date", request.getHeader(D_LABS_DATE_HEADER_NAME));
        } else {
            canonicalHeaders.put("Date", request.getHeader("Date"));
        }

        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            if (headerName.startsWith(D_LABS_HEADER_PREFIX) && !headerName.equals(D_LABS_DATE_HEADER_NAME)) {
                dLabsHeaders.put(headerName, request.getHeader(headerName));
            }
        }

        return generateStringToSign(request.getMethod(), uri, queryParameters, canonicalHeaders, dLabsHeaders);
    }

    private Instant extractTimestampFromRequest(HttpServletRequest request) {
        if (request.getHeader(D_LABS_DATE_HEADER_NAME) != null) {
            return Instant.parse(request.getHeader(D_LABS_DATE_HEADER_NAME));
        } else {
            return Instant.parse(request.getHeader("Date"));
        }
    }

    public static String generateStringToSign(String httpVerb,
                                              String uri, SortedMap<String, String> queryParameters, SortedMap<String, String> canonicalHeaders,
                                              SortedMap<String, String> dLabsHeaders) {
        StringBuilder sb = new StringBuilder();

        sb.append(httpVerb).append("\n");

        for (String headerValue : canonicalHeaders.values()) {
            sb.append(headerValue != null ? headerValue : "").append("\n");
        }

        for (SortedMap.Entry<String, String> entry : dLabsHeaders.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
        }

        sb.append(uri).append("\n");

        for (SortedMap.Entry<String, String> entry : queryParameters.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
        }

        return sb.toString();
    }

    public static String sign(String stringToSign, String signatureKey) {

        byte[] signature = sign(stringToSign.getBytes(UTF8), signatureKey.getBytes(UTF8), SigningAlgorithm.HmacSHA1);

        return Base64.encodeAsString(signature);
    }

    protected static byte[] sign(byte[] data, byte[] key,
                          SigningAlgorithm algorithm) throws AmazonClientException {
        try {
            Mac mac = Mac.getInstance(algorithm.toString());
            mac.init(new SecretKeySpec(key, algorithm.toString()));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Unable to calculate a request signature: "
                            + e.getMessage(), e);
        }
    }

    protected Identity getIdentity() {
        return this.identityInstance.get();
    }

    protected DefaultLoginCredentials getCredentials() {
        return this.credentialsInstance.get();
    }

    private boolean isPrimaryAuthenticationRequest() {
        Object credentials = getCredentials().getCredential();
        return credentials != null && !SignatureTokenCredential.class.isInstance(credentials);
    }

    protected void issueToken(Account account) {
        SignatureTokenProvider tokenProvider = getTokenProvider();

        if (tokenProvider == null) {
            throw new AuthenticationException("No " + Token.Provider.class.getName() + " was found.");
        }

        SignatureToken token = tokenProvider.issue(account);

        getCredentials().setCredential(token);
    }

    protected String renewToken(Account account, SignatureToken renewToken) {
        SignatureTokenProvider tokenProvider = getTokenProvider();

        if (tokenProvider == null) {
            throw new AuthenticationException("No " + Token.Provider.class.getName() + " was found.");
        }

        SignatureToken token = tokenProvider.renew(account, renewToken);

//        getCredentials().setCredential(token);

        return token.getToken();
    }

   protected SignatureTokenCredential createCredential(String username, String signature, String stringToSign, Instant requestTimestamp) {
        SignatureToken token;
        SignatureTokenProvider tokenProvider = getTokenProvider();

        if (tokenProvider != null) {
            token = tokenProvider.get(username);
//            token = (SignatureToken) create(getTokenProvider().getTokenType().getName(), username);
        } else {
            Token.Consumer tokenConsumer = getTokenConsumer();

            if (tokenConsumer == null) {
                throw new AuthenticationException("You must provide a " + Token.Provider.class.getName() + " or " + Token.Consumer.class.getName() + ".");
            }

            token = (SignatureToken) create(getTokenConsumer().getTokenType().getName(), username);
        }

       if (token == null) {
           return null;
//           throw MESSAGES.nullArgument("Token not in cache");
       }

        String generatedSignature = sign(stringToSign, token.getSignatureKey());

        return new SignatureTokenCredential(token, signature, stringToSign, generatedSignature, requestTimestamp);
    }

    protected SignatureTokenProvider getTokenProvider() {
        if (this.tokenProvider.isAmbiguous()) {
            throw new AuthenticationException("You must provide exactly one " + Token.Provider.class.getName() + " implementation.");
        }

        if (!this.tokenProvider.isUnsatisfied()) {
            return (SignatureTokenProvider) this.tokenProvider.get();
        }

        return null;
    }

    protected Token.Consumer getTokenConsumer() {
        if (this.tokenConsumer.isAmbiguous()) {
            throw new AuthenticationException("You must provide exactly one " + Token.Consumer.class.getName() + " implementation.");
        }

        if (!this.tokenConsumer.isUnsatisfied()) {
            return this.tokenConsumer.get();
        }

        return null;
    }
}
