package com.dematic.labs.business.picketlink.idm.credential;

import org.picketlink.idm.credential.AbstractTokenConsumer;

import java.util.Set;

public class TokenConsumer extends AbstractTokenConsumer<SignatureToken> {

    @Override
    protected String extractSubject(SignatureToken token) {
        return null;
    }

    @Override
    protected Set<String> extractRoles(SignatureToken token) {
        return null;
    }

    @Override
    protected Set<String> extractGroups(SignatureToken token) {
        return null;
    }

    @Override
    public boolean validate(SignatureToken token) {
        return false;
    }

    @Override
    public Class<SignatureToken> getTokenType() {
        return SignatureToken.class;
    }
}
