package com.dematic.labs.business.picketlink.idm.credential;

import org.picketlink.idm.credential.AbstractBaseCredentials;

import java.time.Instant;

public class SignatureTokenCredential extends AbstractBaseCredentials {

    private SignatureToken token;

    private final String signature;
    private final String stringToSign;
    private final String generatedSignature;
    private final Instant requestInstant;

    public SignatureTokenCredential(SignatureToken token, String signature, String stringToSign,
                                    String generatedSignature, Instant requestTimestamp) {
        this.token = token;
        this.signature = signature;
        this.stringToSign = stringToSign;
        this.generatedSignature = generatedSignature;
        this.requestInstant = requestTimestamp;
    }

    @Override
    public void invalidate() {
        this.token = null;
    }

    public SignatureToken getToken() {
        return this.token;
    }

    public String getSignature() {
        return signature;
    }

    public String getStringToSign() {
        return stringToSign;
    }

    public String getGeneratedSignature() {
        return generatedSignature;
    }

    public Instant getRequestInstant() {
        return requestInstant;
    }

}
