package com.dematic.labs.rest;

import com.dematic.labs.business.picketlink.SecurityManager;

import javax.ejb.EJB;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

@RequestScoped
@Path("realm")
public class RealmResource {

    @EJB
    SecurityManager securityManager;

    @Context
    UriInfo uriInfo;

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public String[] getList() {
        //noinspection ToArrayCallWithZeroLengthArrayArgument
        return securityManager.supportedRealms().toArray(new String[0]);
    }

}
