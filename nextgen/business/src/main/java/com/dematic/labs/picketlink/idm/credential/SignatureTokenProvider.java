package com.dematic.labs.picketlink.idm.credential;

import org.picketlink.idm.credential.Token;
import org.picketlink.idm.model.Account;
import org.picketlink.idm.model.basic.Agent;

import java.util.HashMap;
import java.util.Map;

public class SignatureTokenProvider implements Token.Provider<SignatureToken> {

    static Map<String, SignatureToken> issuedTokens = new HashMap<>();


    @Override
    public SignatureToken issue(Account account) {

        String loginName = ((Agent)account).getLoginName();

        SignatureToken signatureToken = new SignatureToken(loginName);
        signatureToken.setRealm(account.getPartition().getName());

        issuedTokens.put(loginName, signatureToken);

        return signatureToken;
    }

    @Override
    public SignatureToken renew(Account account, SignatureToken renewToken) {
        String key = ((Agent)account).getLoginName();
        return issuedTokens.get(key);
    }

    @Override
    public void invalidate(Account account) {

    }

    @Override
    public Class<SignatureToken> getTokenType() {
        return SignatureToken.class;
    }



    public SignatureToken get(String key) {

        return issuedTokens.get(key);
    }
}
