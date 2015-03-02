package com.dematic.labs.picketlink.idm.credential.handler;

import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import com.dematic.labs.picketlink.idm.credential.SignatureTokenCredential;
import org.picketlink.common.reflection.Reflections;
import org.picketlink.idm.IdentityManagementException;
import org.picketlink.idm.config.SecurityConfigurationException;
import org.picketlink.idm.credential.Credentials;
import org.picketlink.idm.credential.Token;
import org.picketlink.idm.credential.handler.AbstractCredentialHandler;
import org.picketlink.idm.credential.handler.annotations.SupportsCredentials;
import org.picketlink.idm.credential.storage.CredentialStorage;
import org.picketlink.idm.credential.storage.TokenCredentialStorage;
import org.picketlink.idm.model.Account;
import org.picketlink.idm.spi.CredentialStore;
import org.picketlink.idm.spi.IdentityContext;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@SupportsCredentials(
        credentialClass = { SignatureTokenCredential.class, SignatureToken.class },
        credentialStorage = TokenCredentialStorage.class
)
public class SignatureTokenCredentialHandler <S extends CredentialStore<?>, V extends SignatureTokenCredential, U extends SignatureToken> extends AbstractCredentialHandler<S, V, U> {

    public static final String TOKEN_CONSUMER = "TOKEN_CONSUMER";

    private final List<Token.Consumer> tokenConsumers = new ArrayList<>();

    @Override
    public void setup(S store) {
        super.setup(store);

        Object configuredTokenConsumers = store.getConfig().getCredentialHandlerProperties().get(TOKEN_CONSUMER);

        if (configuredTokenConsumers != null) {
            try {
                if (Token.Consumer.class.isInstance(configuredTokenConsumers)) {
                    this.tokenConsumers.add((Token.Consumer) configuredTokenConsumers);
                } else if (configuredTokenConsumers.getClass().isArray()) {
                    this.tokenConsumers.addAll(Arrays.asList((Token.Consumer[]) configuredTokenConsumers));
                } else if (List.class.isInstance(configuredTokenConsumers)) {
                    //noinspection unchecked
                    this.tokenConsumers.addAll((List<Token.Consumer>) configuredTokenConsumers);
                }
            } catch (ClassCastException cce) {
                throw new SecurityConfigurationException("Token consumer is not a " + Token.Consumer.class.getName() + " instance. You provided " + configuredTokenConsumers);
            }
        }
    }

    @Override
    protected boolean validateCredential(IdentityContext context, CredentialStorage credentialStorage, V credentials, S store) {
        boolean rtnValue;

        rtnValue = credentials.getGeneratedSignature().equals(credentials.getSignature());

        if (rtnValue) {
            Instant now = Instant.now();
            Instant oldestAllowed = now.minusSeconds(15*60);
            Instant requestTimestamp = credentials.getRequestInstant();
            if (requestTimestamp.isBefore(oldestAllowed) || requestTimestamp.isAfter(now)) {
                credentials.setStatus(Credentials.Status.EXPIRED);
                rtnValue = false;
            }
        }

        return rtnValue;
    }

    @Override
    protected Account getAccount(IdentityContext context, V credentials) {
        Token token = credentials.getToken();

        if (token != null) {
            String subject = token.getSubject();

            if (subject == null) {
                throw new IdentityManagementException("No subject returned from token [" + token + "].");
            }

            Account account = getAccount(context, subject);

            if (account == null) {
                account = getAccountById(context, subject);
            }

            return account;
        }

        return null;
    }

    @Override
    protected CredentialStorage getCredentialStorage(IdentityContext context, Account account, V credentials, S store) {
        return null; //store.retrieveCurrentCredential(context, account, getCredentialStorageType());
    }

    @Override
    public CredentialStorage createCredentialStorage(IdentityContext context, Account account, U credential, S store, Date effectiveDate, Date expiryDate) {
        TokenCredentialStorage tokenStorage = createCredentialStorageInstance();

        tokenStorage.setType(credential.getType());
        tokenStorage.setToken(credential.getToken());

        if (effectiveDate != null) {
            tokenStorage.setEffectiveDate(effectiveDate);
        }

        if (tokenStorage.getExpiryDate() == null) {
            tokenStorage.setExpiryDate(expiryDate);
        }

        if (tokenStorage.getType() == null) {
            throw new IdentityManagementException("TokenCredentialStorage can not have a null type.");
        }

        return tokenStorage;
    }

    protected Class<? extends TokenCredentialStorage> getCredentialStorageType() {
        SupportsCredentials supportsCredentials = getClass().getAnnotation(SupportsCredentials.class);
        Class<? extends CredentialStorage> credentialStorage = supportsCredentials.credentialStorage();

        try {
            //noinspection unchecked
            return (Class<? extends TokenCredentialStorage>) credentialStorage;
        } catch (ClassCastException cce) {
            throw new IdentityManagementException("CredentialStorage [" + credentialStorage + "] is not a " + TokenCredentialStorage.class + " type.", cce);
        }
    }

    protected TokenCredentialStorage createCredentialStorageInstance() {
        try {
            return Reflections.newInstance(getCredentialStorageType());
        } catch (Exception e) {
            throw new IdentityManagementException("Could not create TokenStorageCredential [" + getCredentialStorageType() + "].", e);
        }
    }

    private <T extends Token> Token.Consumer<T> getTokenConsumer(T token) {
        for (Token.Consumer selectedConsumer : this.tokenConsumers) {
            //noinspection unchecked
            if (selectedConsumer.getTokenType().isAssignableFrom(token.getClass())) {
                //noinspection unchecked
                return selectedConsumer;
            }
        }

        return null;
    }
}
