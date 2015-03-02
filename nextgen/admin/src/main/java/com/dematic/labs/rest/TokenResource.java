package com.dematic.labs.rest;

import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import org.picketlink.credential.DefaultLoginCredentials;

import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@RequestScoped
@Path("token")
public class TokenResource {

    @Inject
    private Instance<DefaultLoginCredentials> credentialsInstance;

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public SignatureToken getList() {
        return (SignatureToken) getCredentials().getCredential();
    }

    protected DefaultLoginCredentials getCredentials() {
        return this.credentialsInstance.get();
    }

}
