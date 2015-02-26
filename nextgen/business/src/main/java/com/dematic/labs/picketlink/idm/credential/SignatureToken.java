package com.dematic.labs.picketlink.idm.credential;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.UUID;

@XmlRootElement
public class SignatureToken implements org.picketlink.idm.credential.Token { //extends AbstractToken {

    private String signatureKey;
    private final String type = getClass().getName();
    private String token;
    private String realm;

    @SuppressWarnings("UnusedDeclaration") //needed for jackson
    public SignatureToken() {
    }

    public SignatureToken(String token) {
        this.token = token;
        this.signatureKey = UUID.randomUUID().toString();
    }

    @Override
    @JsonIgnore
    public String getType() {
        return type;
    }

    @Override
    @JsonIgnore
    public String getSubject() {
        return getToken();
    }

    public String getSignatureKey() {
        return signatureKey;
    }

    public void setSignatureKey(String signatureKey) {
        this.signatureKey = signatureKey;
    }

    @Override
    public String getToken() {
        return this.token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getRealm() {
        return realm;
    }

    public void setRealm(String realm) {
        this.realm = realm;
    }
}
