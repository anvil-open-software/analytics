package com.dematic.labs.rest;

import com.dematic.labs.business.SecurityManager;
import com.dematic.labs.business.dto.TenantDto;

import javax.ejb.EJB;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.List;

@RequestScoped
@Path("tenant")
public class TenantResource {

    @EJB
    SecurityManager securityManager;

    @Context
    UriInfo uriInfo;

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public List<TenantDto> getList() {
        return securityManager.getTenants();
    }

}
