package com.dematic.labs.rest;

import com.dematic.labs.business.SecurityManager;
import com.dematic.labs.business.dto.RoleDto;
import com.dematic.labs.picketlink.idm.credential.SignatureToken;
import org.picketlink.credential.DefaultLoginCredentials;

import javax.ejb.EJB;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.stream.Collectors;

@RequestScoped
@Path("token")
public class TokenResource {

    @EJB
    SecurityManager securityManager;

    @Inject
    private Instance<DefaultLoginCredentials> credentialsInstance;

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public SignatureToken getToken() {
        SignatureToken token = (SignatureToken) getCredentials().getCredential();

        token.setGrantedRoles(securityManager.getAuthenticatedUser().getGrantedRoles()
            .stream().map(RoleDto::getName).collect(Collectors.toList()));

        return token;
    }

    protected DefaultLoginCredentials getCredentials() {
        return this.credentialsInstance.get();
    }

}
