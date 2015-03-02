package com.dematic.labs.rest;

import com.dematic.labs.business.SecurityManager;
import com.dematic.labs.business.dto.TenantDto;
import org.picketlink.authorization.annotations.RolesAllowed;

import javax.ejb.EJB;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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
    @RolesAllowed("administerTenants")
    public List<TenantDto> getList() {
        return securityManager.getTenants();
    }


    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @RolesAllowed("administerTenants")
    public Response create(TenantDto tenantDto) {
        TenantDto returnedTenantDto = securityManager.createTenant(tenantDto);
        return Response.created(uriInfo.getAbsolutePathBuilder().path(returnedTenantDto.getId()).build())
                .entity(returnedTenantDto).build();
    }



}
